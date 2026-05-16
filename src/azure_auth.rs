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

/// Azure credential configuration for database connections
#[pyclass(name = "AzureCredential", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyAzureCredential {
    pub credential_type: AzureCredentialType,
    pub config: HashMap<String, String>,
    // Allows N concurrent readers (fast-path cache hits) while serialising writes.
    token_cache: Arc<RwLock<Option<CachedToken>>>,
    // Serialises the network refresh step without holding the RwLock across an await.
    // Only one task performs the HTTP round-trip; every other expired-token caller
    // queues here and then finds a fresh token on its double-check read.
    refresh_mutex: Arc<Mutex<()>>,
    // Shared, connection-pooling HTTP client — built once, reused on every refresh.
    // reqwest::Client is Arc-based internally and is both Clone and Send + Sync.
    client: Client,
}

/// Types of Azure credentials supported
#[pyclass(name = "AzureCredentialType", from_py_object)]
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

/// Build the shared HTTP client used by all token-refresh paths.
/// A 30-second timeout is applied at the client level so individual
/// requests do not need to set it themselves.
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
        // Return sanitized config that redacts sensitive fields
        let mut sanitized = self.config.clone();

        // Redact sensitive fields
        if sanitized.contains_key("client_secret") {
            sanitized.insert("client_secret".to_string(), "***REDACTED***".to_string());
        }
        if sanitized.contains_key("access_token") {
            sanitized.insert("access_token".to_string(), "***REDACTED***".to_string());
        }

        sanitized
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
    // Private helper method for internal access to raw config values
    fn get_config_value(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    pub async fn to_auth_method(&self) -> PyResult<AuthMethod> {
        // ── Static access tokens bypass all caching logic ────────────────────────
        if let AzureCredentialType::AccessToken = self.credential_type {
            let token = self
                .get_config_value("access_token")
                .ok_or_else(|| PyValueError::new_err("Access token not found in configuration"))?;
            return Ok(AuthMethod::aad_token(token));
        }

        // ── Fast path: shared read lock (N concurrent readers allowed) ───────────
        // Clone the token string before releasing the guard — we must not hold a
        // read lock across the async operations below.
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if cached.expires_at > Instant::now() {
                    let token = cached.access_token.clone();
                    return Ok(AuthMethod::aad_token(&token));
                }
            }
        } // read_guard released here — no RwLock is held past this point

        // ── Slow path: serialise refresh via a dedicated async mutex ─────────────
        //
        // Pattern: double-checked locking without holding the RwLock across I/O.
        //
        // 1. Acquire the lightweight refresh_mutex — only one task proceeds to
        //    the network; every other expired-token caller lines up here.
        // 2. Re-check the cache under a read lock inside the mutex (double-check).
        //    If a previous waiter already refreshed the token, serve from cache.
        // 3. If still expired, perform the HTTP round-trip with NO RwLock held.
        // 4. Open a write lock only for the brief struct-assignment at the end.
        //
        // Result: the RwLock write guard is held for nanoseconds, never milliseconds.
        let _refresh_guard = self.refresh_mutex.lock().await;

        // Double-check: another waiter may have refreshed while we queued.
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if cached.expires_at > Instant::now() {
                    let token = cached.access_token.clone();
                    return Ok(AuthMethod::aad_token(&token));
                }
            }
        } // read_guard released — still no RwLock held

        // Perform the network request with no lock of any kind held.
        let (token, expires_in) = match self.credential_type {
            AzureCredentialType::ServicePrincipal => {
                let client_id = self
                    .get_config_value("client_id")
                    .ok_or_else(|| PyValueError::new_err("Client ID not found in configuration"))?;
                let client_secret = self.get_config_value("client_secret").ok_or_else(|| {
                    PyValueError::new_err("Client secret not found in configuration")
                })?;
                let tenant_id = self
                    .get_config_value("tenant_id")
                    .ok_or_else(|| PyValueError::new_err("Tenant ID not found in configuration"))?;
                self.acquire_service_principal_token(client_id, client_secret, tenant_id)
                    .await?
            }
            AzureCredentialType::ManagedIdentity => {
                let client_id = self.get_config_value("client_id");
                self.acquire_managed_identity_token(client_id.cloned())
                    .await?
            }
            AzureCredentialType::DefaultAzure => self.acquire_default_azure_token().await?,
            AzureCredentialType::AccessToken => unreachable!(), // Handled above
        };

        // Clamp the safety buffer so it never exceeds expires_in (subtraction overflow guard).
        let buffer_secs = ((expires_in as f64 * 0.10) as u64)
            .max(30)
            .min(600)
            .min(expires_in);
        let expires_at =
            Instant::now() + Duration::from_secs(expires_in.saturating_sub(buffer_secs));

        // Brief write lock: struct assignment only — no I/O.
        {
            let mut write_guard = self.token_cache.write().await;
            *write_guard = Some(CachedToken {
                access_token: token.clone(),
                expires_at,
            });
        } // write_guard dropped here — _refresh_guard released when this fn returns

        Ok(AuthMethod::aad_token(&token))
    }

    async fn acquire_service_principal_token(
        &self,
        client_id: &str,
        client_secret: &str,
        tenant_id: &str,
    ) -> PyResult<(String, u64)> {
        let token_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id
        );

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "https://database.windows.net/.default"),
        ];

        // .form() sets Content-Type: application/x-www-form-urlencoded automatically
        // and handles URL encoding — no need for serde_urlencoded or manual headers.
        let response = self
            .client
            .post(&token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(PyRuntimeError::new_err(format!(
                "Failed to acquire Service Principal token. Status: {}, Response: {}",
                status, error_text
            )));
        }

        let json: Value = response.json().await.map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to parse token response: {}", e))
        })?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token not found in response"))?;

        let expires_in = json["expires_in"].as_u64().unwrap_or(3600); // Default to 1 hour if expires_in is missing or invalid

        Ok((access_token.to_string(), expires_in))
    }

    async fn acquire_managed_identity_token(
        &self,
        client_id: Option<String>,
    ) -> PyResult<(String, u64)> {
        // Azure Instance Metadata Service (IMDS) endpoint: official Azure endpoint for
        // accessing metadata and managed identity tokens from Azure compute resources.
        //
        // NOTE: HTTP (not HTTPS) is required by Azure IMDS and is safe here because:
        // - 169.254.169.254 is a link-local address only reachable from within the resource
        // - Traffic stays on the Azure host and never traverses external networks
        // - This HTTP endpoint is the standard, documented mechanism used by Azure SDKs
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

        // Timeout is already set on self.client (30 s); no per-request override needed.
        let response = self
            .client
            .get(url)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(PyRuntimeError::new_err(format!(
                "Failed to acquire Managed Identity token. Status: {}, Response: {}. Ensure managed identity is enabled and assigned to this resource.",
                status, error_text
            )));
        }

        let json: Value = response.json().await.map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to parse token response: {}", e))
        })?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token not found in response"))?;

        let expires_in = json["expires_in"].as_u64().unwrap_or(3600); // Default to 1 hour if expires_in is missing or invalid

        Ok((access_token.to_string(), expires_in))
    }

    async fn acquire_default_azure_token(&self) -> PyResult<(String, u64)> {
        // Try environment variables first (Service Principal)
        if let (Ok(client_id), Ok(client_secret), Ok(tenant_id)) = (
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_CLIENT_SECRET"),
            std::env::var("AZURE_TENANT_ID"),
        ) {
            return self
                .acquire_service_principal_token(&client_id, &client_secret, &tenant_id)
                .await;
        }

        // Try Managed Identity if environment variables not present
        if let Ok((token, expires_in)) = self.acquire_managed_identity_token(None).await {
            return Ok((token, expires_in));
        }

        match tokio::process::Command::new("az")
            .args([
                "account",
                "get-access-token",
                "--resource",
                "https://database.windows.net/",
                "--output",
                "json",
            ])
            .output()
            .await
        {
            Ok(output) if output.status.success() => {
                let json: Value = serde_json::from_slice(&output.stdout).map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to parse Azure CLI output: {}", e))
                })?;

                let access_token = json["accessToken"].as_str().ok_or_else(|| {
                    PyRuntimeError::new_err("Access token not found in Azure CLI response")
                })?;

                // Parse the expiresOn timestamp from Azure CLI response.
                // `az account get-access-token` emits this field as
                // "YYYY-MM-DD HH:MM:SS.ffffff" (space separator, no timezone);
                // it is NOT RFC 3339 and parse_from_rfc3339 always fails on it.
                let expires_on = json["expiresOn"].as_str().ok_or_else(|| {
                    PyRuntimeError::new_err("expiresOn not found in Azure CLI response")
                })?;

                let expires_at =
                    chrono::NaiveDateTime::parse_from_str(expires_on, "%Y-%m-%d %H:%M:%S%.f")
                        .map_err(|e| {
                            PyRuntimeError::new_err(format!(
                                "Failed to parse expiresOn '{}': {}",
                                expires_on, e
                            ))
                        })?;

                let now_naive = chrono::Local::now().naive_local();
                let expires_in = expires_at
                    .signed_duration_since(now_naive)
                    .num_seconds()
                    .max(0) as u64;

                if expires_in == 0 {
                    return Err(PyRuntimeError::new_err(format!(
                        "Azure CLI returned an already-expired token (expiresOn: {}). \
                         Run 'az login' to refresh your credentials.",
                        expires_on
                    )));
                }

                Ok((access_token.to_string(), expires_in))
            }
            Ok(output) => {
                let error_msg = String::from_utf8_lossy(&output.stderr);
                Err(PyRuntimeError::new_err(format!(
                    "Azure CLI failed: {}. Run 'az login' to authenticate.",
                    error_msg
                )))
            }
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to execute Azure CLI: {}. Ensure Azure credentials are configured:\n\
                 1. Set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID environment variables, or\n\
                 2. Enable managed identity on Azure resource, or\n\
                 3. Install Azure CLI and run 'az login'",
                e
            ))),
        }
    }
}
