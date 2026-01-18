use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiberius::AuthMethod;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Azure credential configuration for database connections
#[pyclass(name = "AzureCredential")]
#[derive(Clone, Debug)]
pub struct PyAzureCredential {
    pub credential_type: AzureCredentialType,
    pub config: HashMap<String, String>,
    // Thread-safe token cache
    token_cache: Arc<Mutex<Option<CachedToken>>>,
}

/// Types of Azure credentials supported
#[pyclass(name = "AzureCredentialType")]
#[derive(Clone, Debug, PartialEq)]
pub enum AzureCredentialType {
    ServicePrincipal,
    ManagedIdentity,
    AccessToken,
    DefaultAzure,
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

#[pymethods]
impl PyAzureCredential {
    #[staticmethod]
    pub fn service_principal(
        client_id: String,
        client_secret: String,
        tenant_id: String,
    ) -> Self {
        let mut config = HashMap::new();
        config.insert("client_id".to_string(), client_id);
        config.insert("client_secret".to_string(), client_secret);
        config.insert("tenant_id".to_string(), tenant_id);

        PyAzureCredential {
            credential_type: AzureCredentialType::ServicePrincipal,
            config,
            token_cache: Arc::new(Mutex::new(None)),
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
            token_cache: Arc::new(Mutex::new(None)),
        }
    }

    #[staticmethod]
    pub fn access_token(token: String) -> Self {
        let mut config = HashMap::new();
        config.insert("access_token".to_string(), token);

        PyAzureCredential {
            credential_type: AzureCredentialType::AccessToken,
            config,
            token_cache: Arc::new(Mutex::new(None)),
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        PyAzureCredential {
            credential_type: AzureCredentialType::DefaultAzure,
            config: HashMap::new(),
            token_cache: Arc::new(Mutex::new(None)),
        }
    }

    #[getter]
    pub fn credential_type(&self) -> AzureCredentialType {
        self.credential_type.clone()
    }

    #[getter]
    pub fn config(&self) -> HashMap<String, String> {
        self.config.clone()
    }

    pub fn __str__(&self) -> String {
        format!(
            "AzureCredential(type={}, config_keys={:?})",
            self.credential_type.__str__(),
            self.config.keys().collect::<Vec<_>>()
        )
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl PyAzureCredential {
    pub async fn to_auth_method(&self) -> PyResult<AuthMethod> {
        // For static access tokens, return directly without caching
        if let AzureCredentialType::AccessToken = self.credential_type {
            let token = self.config.get("access_token")
                .ok_or_else(|| PyValueError::new_err("Access token not found in configuration"))?;
            return Ok(AuthMethod::aad_token(token));
        }

        {
            let cache_guard = self.token_cache.lock().await;
            if let Some(cached) = cache_guard.as_ref() {
                // Check if token is still valid (with 5 minute buffer before expiry)
                if cached.expires_at > Instant::now() + Duration::from_secs(300) {
                    return Ok(AuthMethod::aad_token(&cached.access_token));
                }
            }
        }

        let token = match self.credential_type {
            AzureCredentialType::ServicePrincipal => {
                let client_id = self.config.get("client_id")
                    .ok_or_else(|| PyValueError::new_err("Client ID not found in configuration"))?;
                let client_secret = self.config.get("client_secret")
                    .ok_or_else(|| PyValueError::new_err("Client secret not found in configuration"))?;
                let tenant_id = self.config.get("tenant_id")
                    .ok_or_else(|| PyValueError::new_err("Tenant ID not found in configuration"))?;

                self.acquire_service_principal_token_cached(client_id, client_secret, tenant_id).await?
            }
            AzureCredentialType::ManagedIdentity => {
                let client_id = self.config.get("client_id");
                self.acquire_managed_identity_token_cached(client_id.cloned()).await?
            }
            AzureCredentialType::DefaultAzure => {
                self.acquire_default_azure_token_cached().await?
            }
            AzureCredentialType::AccessToken => unreachable!(), // Handled above
        };

        Ok(AuthMethod::aad_token(token))
    }

    // Cached token acquisition methods
    async fn acquire_service_principal_token_cached(
        &self,
        client_id: &str,
        client_secret: &str,
        tenant_id: &str,
    ) -> PyResult<String> {
        let token = self.acquire_service_principal_token(client_id, client_secret, tenant_id).await?;
        self.cache_token(token.clone()).await;
        Ok(token)
    }

    async fn acquire_managed_identity_token_cached(&self, client_id: Option<String>) -> PyResult<String> {
        let token = self.acquire_managed_identity_token(client_id).await?;
        self.cache_token(token.clone()).await;
        Ok(token)
    }

    async fn acquire_default_azure_token_cached(&self) -> PyResult<String> {
        let token = self.acquire_default_azure_token().await?;
        self.cache_token(token.clone()).await;
        Ok(token)
    }

    async fn cache_token(&self, access_token: String) {
        // Azure tokens typically last 1 hour, we'll cache for 55 minutes to be safe
        let expires_at = Instant::now() + Duration::from_secs(55 * 60);
        let cached_token = CachedToken {
            access_token,
            expires_at,
        };

        let mut cache_guard = self.token_cache.lock().await;
        *cache_guard = Some(cached_token);
    }

    async fn acquire_service_principal_token(
        &self,
        client_id: &str,
        client_secret: &str,
        tenant_id: &str,
    ) -> PyResult<String> {
        let client = Client::new();
        let token_url = format!("https://login.microsoftonline.com/{}/oauth2/v2.0/token", tenant_id);
        
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "https://database.windows.net/.default"),
        ];

        let response = client
            .post(&token_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(serde_urlencoded::to_string(&params).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to encode form params: {}", e))
            })?)
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(PyRuntimeError::new_err(format!(
                "Failed to acquire Service Principal token. Status: {}, Response: {}",
                status,
                error_text
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse token response: {}", e)))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token not found in response"))?;

        Ok(access_token.to_string())
    }

    async fn acquire_managed_identity_token(&self, client_id: Option<String>) -> PyResult<String> {
        let client = Client::new();
        const IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
        const API_VERSION: &str = "2021-02-01"; // Current recommended API version
        
        let mut url = reqwest::Url::parse(IMDS_ENDPOINT)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid IMDS URL: {}", e)))?;
        
        url.query_pairs_mut()
            .append_pair("api-version", API_VERSION)
            .append_pair("resource", "https://database.windows.net/");
            
        if let Some(ref id) = client_id {
            url.query_pairs_mut().append_pair("client_id", id);
        }
            
        let response = client
            .get(url)
            .header("Metadata", "true")
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(PyRuntimeError::new_err(format!(
                "Failed to acquire Managed Identity token. Status: {}, Response: {}. Ensure managed identity is enabled and assigned to this resource.",
                status,
                error_text
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse token response: {}", e)))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token not found in response"))?;

        Ok(access_token.to_string())
    }

    async fn acquire_default_azure_token(&self) -> PyResult<String> {
        // Try environment variables first (Service Principal)
        if let (Ok(client_id), Ok(client_secret), Ok(tenant_id)) = (
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_CLIENT_SECRET"),
            std::env::var("AZURE_TENANT_ID"),
        ) {
            return self.acquire_service_principal_token(&client_id, &client_secret, &tenant_id).await;
        }

        // Try Managed Identity if environment variables not present
        if let Ok(token) = self.acquire_managed_identity_token(None).await {
            return Ok(token);
        }

        match tokio::process::Command::new("az")
            .args(["account", "get-access-token", "--resource", "https://database.windows.net/", "--output", "json"])
            .output()
            .await
        {
            Ok(output) if output.status.success() => {
                let json: Value = serde_json::from_slice(&output.stdout)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse Azure CLI output: {}", e)))?;
                
                let access_token = json["accessToken"]
                    .as_str()
                    .ok_or_else(|| PyRuntimeError::new_err("Access token not found in Azure CLI response"))?;
                
                Ok(access_token.to_string())
            },
            Ok(output) => {
                let error_msg = String::from_utf8_lossy(&output.stderr);
                Err(PyRuntimeError::new_err(format!(
                    "Azure CLI failed: {}. Run 'az login' to authenticate.", error_msg
                )))
            },
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to execute Azure CLI: {}. Ensure Azure credentials are configured:\n\
                 1. Set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID environment variables, or\n\
                 2. Enable managed identity on Azure resource, or\n\
                 3. Install Azure CLI and run 'az login'", e
            ))),
        }
    }
}