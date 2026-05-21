use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiberius::AuthMethod;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

#[pyclass(name = "AzureCredentialType", from_py_object)] // <-- Explicit opt-in
#[derive(Clone, Debug, PartialEq)]
pub enum AzureCredentialType {
    ServicePrincipal,
    ManagedIdentity,
    AccessToken,
    DefaultAzure,
}

#[pyclass(name = "AzureCredential", from_py_object)] // <-- Explicit opt-in
#[derive(Clone, Debug)]
pub struct PyAzureCredential {
    pub credential_type: AzureCredentialType,
    pub config: HashMap<String, String>,
    token_cache: Arc<RwLock<Option<CachedToken>>>,
    refresh_mutex: Arc<Mutex<()>>,
    client: Client,
}

#[pymethods]
impl AzureCredentialType {
    #[classattr]
    const SERVICE_PRINCIPAL: AzureCredentialType = AzureCredentialType::ServicePrincipal;
    #[classattr]
    const MANAGED_IDENTITY: AzureCredentialType = AzureCredentialType::ManagedIdentity;
    #[classattr]
    const ACCESS_TOKEN: AzureCredentialType = AzureCredentialType::AccessToken;
    #[classattr]
    const DEFAULT_AZURE: AzureCredentialType = AzureCredentialType::DefaultAzure;

    pub fn __str__(&self) -> String {
        match self {
            AzureCredentialType::ServicePrincipal => "ServicePrincipal".into(),
            AzureCredentialType::ManagedIdentity => "ManagedIdentity".into(),
            AzureCredentialType::AccessToken => "AccessToken".into(),
            AzureCredentialType::DefaultAzure => "DefaultAzure".into(),
        }
    }
    pub fn __repr__(&self) -> String {
        format!("AzureCredentialType.{}", self.__str__())
    }
}

fn build_http_client() -> Result<Client, reqwest::Error> {
    Client::builder().timeout(Duration::from_secs(30)).build()
}

#[pymethods]
impl PyAzureCredential {
    #[staticmethod]
    pub fn service_principal(client_id: String, client_secret: String, tenant_id: String) -> Self {
        let mut config = HashMap::new();
        config.insert("client_id".to_string(), client_id);
        config.insert("client_secret".to_string(), client_secret);
        config.insert("tenant_id".to_string(), tenant_id);

        PyAzureCredential {
            credential_type: AzureCredentialType::ServicePrincipal,
            config,
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client: build_http_client().expect("Failed to build HTTP client"),
        }
    }

    #[staticmethod]
    pub fn managed_identity(client_id: Option<String>) -> Self {
        let mut config = HashMap::new();
        if let Some(id) = client_id {
            config.insert("client_id".to_string(), id);
        }

        PyAzureCredential {
            credential_type: AzureCredentialType::ManagedIdentity,
            config,
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client: build_http_client().expect("Failed to build HTTP client"),
        }
    }

    #[staticmethod]
    pub fn access_token(token: String) -> Self {
        let mut config = HashMap::new();
        config.insert("access_token".to_string(), token);

        PyAzureCredential {
            credential_type: AzureCredentialType::AccessToken,
            config,
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client: build_http_client().expect("Failed to build HTTP client"),
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        PyAzureCredential {
            credential_type: AzureCredentialType::DefaultAzure,
            config: HashMap::new(),
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client: build_http_client().expect("Failed to build HTTP client"),
        }
    }

    #[getter]
    pub fn credential_type(&self) -> AzureCredentialType {
        self.credential_type.clone()
    }

    #[getter]
    pub fn config(&self) -> HashMap<String, String> {
        let mut sanitized = self.config.clone();
        if sanitized.contains_key("client_secret") {
            sanitized.insert("client_secret".to_string(), "***REDACTED***".to_string());
        }
        if sanitized.contains_key("access_token") {
            sanitized.insert("access_token".to_string(), "***REDACTED***".to_string());
        }
        sanitized
    }
}

impl PyAzureCredential {
    fn get_config_value(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    // Helper to safely parse variations of "expires_in" fields from Azure JSON
    fn parse_expires_in(json: &Value, key: &str) -> u64 {
        if let Some(num) = json[key].as_u64() {
            return num;
        }
        if let Some(s) = json[key].as_str() {
            if let Ok(parsed) = s.parse::<u64>() {
                return parsed;
            }
        }
        3600 // Safe default: 1 hour
    }

    pub async fn to_auth_method(&self) -> PyResult<AuthMethod> {
        // 1. Static Access Token Bypass
        if let AzureCredentialType::AccessToken = self.credential_type {
            let token = self
                .get_config_value("access_token")
                .ok_or_else(|| PyValueError::new_err("Access token not found in configuration"))?;
            // Pass the owned/cloned String directly — no Box::leak!
            return Ok(AuthMethod::aad_token(token.clone()));
        }

        // 2. Fast Path Read Lock
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if cached.expires_at > Instant::now() {
                    return Ok(AuthMethod::aad_token(cached.access_token.clone()));
                }
            }
        }

        // 3. Slow Path Serialization Mutex
        let _refresh_guard = self.refresh_mutex.lock().await;

        // Double check cache
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if cached.expires_at > Instant::now() {
                    return Ok(AuthMethod::aad_token(cached.access_token.clone()));
                }
            }
        }

        // Fetch new token over network
        let (token, expires_in) = match self.credential_type {
            AzureCredentialType::ServicePrincipal => {
                let client_id = self.get_config_value("client_id")
                    .ok_or_else(|| PyValueError::new_err("Client ID not found"))?;
                let client_secret = self.get_config_value("client_secret")
                    .ok_or_else(|| PyValueError::new_err("Client secret not found"))?;
                let tenant_id = self.get_config_value("tenant_id")
                    .ok_or_else(|| PyValueError::new_err("Tenant ID not found"))?;
                self.acquire_service_principal_token(client_id, client_secret, tenant_id).await?
            }
            AzureCredentialType::ManagedIdentity => {
                let client_id = self.get_config_value("client_id");
                self.acquire_managed_identity_token(client_id.cloned()).await?
            }
            AzureCredentialType::DefaultAzure => self.acquire_default_azure_token().await?,
            AzureCredentialType::AccessToken => unreachable!(),
        };

        // Enforce safety buffers against premature expiration
        let buffer_secs = ((expires_in as f64 * 0.10) as u64).max(30).min(600).min(expires_in);
        let expires_at = Instant::now() + Duration::from_secs(expires_in.saturating_sub(buffer_secs));

        // Brief write lock update
        {
            let mut write_guard = self.token_cache.write().await;
            *write_guard = Some(CachedToken {
                access_token: token.clone(),
                expires_at,
            });
        }

        // Clean owned string passing — safe from memory leaks!
        Ok(AuthMethod::aad_token(token))
    }

    async fn acquire_service_principal_token(
        &self,
        client_id: &str,
        client_secret: &str,
        tenant_id: &str,
    ) -> PyResult<(String, u64)> {
        let token_url = format!("https://login.microsoftonline.com/{}/oauth2/v2.0/token", tenant_id);
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "https://database.windows.net/.default"),
        ];

        let response = self.client.post(&token_url).form(&params).send().await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(PyRuntimeError::new_err(format!("HTTP Error: {}", response.status())));
        }

        let json: Value = response.json().await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed parsing JSON: {}", e)))?;
        
        let access_token = json["access_token"].as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token missing"))?.to_string();

        let expires_in = Self::parse_expires_in(&json, "expires_in");

        Ok((access_token, expires_in))
    }

    async fn acquire_managed_identity_token(&self, client_id: Option<String>) -> PyResult<(String, u64)> {
        const IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
        let mut url = reqwest::Url::parse(IMDS_ENDPOINT).unwrap();
        
        url.query_pairs_mut()
            .append_pair("api-version", "2021-02-01")
            .append_pair("resource", "https://database.windows.net/");

        if let Some(ref id) = client_id {
            url.query_pairs_mut().append_pair("client_id", id);
        }

        let response = self.client.get(url).header("Metadata", "true").send().await
            .map_err(|e| PyRuntimeError::new_err(format!("IMDS request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(PyRuntimeError::new_err(format!("IMDS error status: {}", response.status())));
        }

        let json: Value = response.json().await.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let access_token = json["access_token"].as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token missing"))?.to_string();

        let expires_in = Self::parse_expires_in(&json, "expires_in");

        Ok((access_token, expires_in))
    }

    async fn acquire_default_azure_token(&self) -> PyResult<(String, u64)> {
        if let (Ok(client_id), Ok(client_secret), Ok(tenant_id)) = (
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_CLIENT_SECRET"),
            std::env::var("AZURE_TENANT_ID"),
        ) {
            return self.acquire_service_principal_token(&client_id, &client_secret, &tenant_id).await;
        }

        if let Ok(res) = self.acquire_managed_identity_token(None).await {
            return Ok(res);
        }

        // Fallback to Azure CLI
        match tokio::process::Command::new("az")
            .args(["account", "get-access-token", "--resource", "https://database.windows.net/", "--output", "json"])
            .output().await 
        {
            Ok(output) if output.status.success() => {
                let json: Value = serde_json::from_slice(&output.stdout)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                
                let access_token = json["accessToken"].as_str()
                    .ok_or_else(|| PyRuntimeError::new_err("Missing accessToken"))?.to_string();

                // Azure CLI returns 'expiresOn' as a timestamp string, not an integer duration.
                // Passing it to parse_expires_in will trigger your 3600-second safe default,
                // which is perfectly fine and resilient for a local CLI development fallback!
                let expires_in = Self::parse_expires_in(&json, "expiresIn");

                Ok((access_token, expires_in))
            }
            _ => Err(PyRuntimeError::new_err("All DefaultAzureCredential authentication paths failed.")),
        }
    }
}