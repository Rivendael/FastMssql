use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiberius::AuthMethod;
use tokio::sync::{Mutex, RwLock};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Secure string wrapper that zeroizes memory when dropped
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
struct SensitiveString(String);

impl SensitiveString {
    fn new(s: String) -> Self {
        SensitiveString(s)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SensitiveString(***redacted***)")
    }
}

#[derive(Clone)]
struct CachedToken {
    access_token: SensitiveString,
    expires_at: Instant,
}

impl fmt::Debug for CachedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedToken")
            .field("access_token", &self.access_token)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Drop for CachedToken {
    fn drop(&mut self) {
        // SensitiveString will be zeroized on drop automatically
    }
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
#[derive(Clone)]
pub struct PyAzureCredential {
    pub credential_type: AzureCredentialType,
    // Non-sensitive configuration (safe to store)
    pub config: HashMap<String, String>,
    // Sensitive configuration (zeroized on drop; never exposed via .config)
    sensitive_config: Arc<HashMap<String, SensitiveString>>,
    token_cache: Arc<RwLock<Option<CachedToken>>>,
    refresh_mutex: Arc<Mutex<()>>,
    client: Arc<Client>,
}

impl fmt::Debug for PyAzureCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyAzureCredential")
            .field("credential_type", &self.credential_type)
            .field("config", &self.config)
            .field("sensitive_config", &"***redacted***")
            .field("token_cache", &"***redacted***")
            .finish()
    }
}

impl Drop for PyAzureCredential {
    fn drop(&mut self) {
        // Explicitly clear sensitive config when credential is dropped
        // Arc will handle the cleanup when this is the last reference
        // The inner SensitiveString items will be zeroized on drop
    }
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

fn build_http_client() -> Result<Arc<Client>, reqwest::Error> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()?;
    Ok(Arc::new(client))
}

#[pymethods]
impl PyAzureCredential {
    #[staticmethod]
    pub fn service_principal(
        client_id: String,
        client_secret: String,
        tenant_id: String,
    ) -> PyResult<Self> {
        let mut config = HashMap::new();
        config.insert("client_id".to_string(), client_id.clone());
        config.insert("tenant_id".to_string(), tenant_id.clone());

        let mut sensitive_config = HashMap::new();
        sensitive_config.insert("client_id".to_string(), SensitiveString::new(client_id));
        sensitive_config.insert(
            "client_secret".to_string(),
            SensitiveString::new(client_secret),
        );
        sensitive_config.insert("tenant_id".to_string(), SensitiveString::new(tenant_id));

        let client = build_http_client()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))?;

        Ok(PyAzureCredential {
            credential_type: AzureCredentialType::ServicePrincipal,
            config,
            sensitive_config: Arc::new(sensitive_config),
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client,
        })
    }

    #[staticmethod]
    pub fn managed_identity(client_id: Option<String>) -> PyResult<Self> {
        let mut config = HashMap::new();
        let mut sensitive_config = HashMap::new();

        if let Some(id) = client_id {
            config.insert("client_id".to_string(), id.clone());
            sensitive_config.insert("client_id".to_string(), SensitiveString::new(id));
        }

        let client = build_http_client()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))?;

        Ok(PyAzureCredential {
            credential_type: AzureCredentialType::ManagedIdentity,
            config,
            sensitive_config: Arc::new(sensitive_config),
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client,
        })
    }

    #[staticmethod]
    pub fn access_token(token: String) -> PyResult<Self> {
        let mut sensitive_config = HashMap::new();
        sensitive_config.insert("access_token".to_string(), SensitiveString::new(token));

        let client = build_http_client()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))?;

        Ok(PyAzureCredential {
            credential_type: AzureCredentialType::AccessToken,
            config: HashMap::new(),
            sensitive_config: Arc::new(sensitive_config),
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client,
        })
    }

    #[staticmethod]
    pub fn default() -> PyResult<Self> {
        let client = build_http_client()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))?;

        Ok(PyAzureCredential {
            credential_type: AzureCredentialType::DefaultAzure,
            config: HashMap::new(),
            sensitive_config: Arc::new(HashMap::new()),
            token_cache: Arc::new(RwLock::new(None)),
            refresh_mutex: Arc::new(Mutex::new(())),
            client,
        })
    }

    #[getter]
    pub fn credential_type(&self) -> AzureCredentialType {
        self.credential_type.clone()
    }

    #[getter]
    pub fn config(&self) -> HashMap<String, String> {
        // Return sanitized config without exposing sensitive data
        self.config.clone()
    }
}

impl PyAzureCredential {
    fn get_sensitive_value(&self, key: &str) -> Option<&SensitiveString> {
        self.sensitive_config.get(key)
    }

    /// Check if cached token is still valid.
    ///
    /// Note: `expires_at` is already written with a safety buffer applied (see the
    /// `buffer_secs` logic when caching), so we only need a simple comparison here.
    fn is_token_still_valid(cached_token: &CachedToken) -> bool {
        Instant::now() < cached_token.expires_at
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

    // Parse Azure CLI's "expiresOn" timestamp string (ISO 8601) and calculate expires_in
    fn parse_expires_on(json: &Value, key: &str) -> u64 {
        if let Some(timestamp_str) = json[key].as_str() {
            // Try to parse ISO 8601 timestamp (e.g., "2026-01-18T12:00:00Z")
            if let Ok(expires_dt) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
                let now = chrono::Utc::now();
                let expires_utc = expires_dt.with_timezone(&chrono::Utc);
                
                // Calculate duration in seconds. If already expired, return 0.
                if let Ok(duration) = expires_utc.signed_duration_since(now).to_std() {
                    return duration.as_secs();
                }
            }
        }
        3600 // Safe default: 1 hour if parsing fails
    }

    pub async fn to_auth_method(&self) -> PyResult<AuthMethod> {
        // 1. Static Access Token Bypass
        if let AzureCredentialType::AccessToken = self.credential_type {
            let token = self
                .get_sensitive_value("access_token")
                .ok_or_else(|| PyValueError::new_err("Access token not found in configuration"))?;
            // Pass reference, let AuthMethod handle cloning if needed
            return Ok(AuthMethod::aad_token(token.as_str().to_string()));
        }

        // 2. Fast Path Read Lock
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if Self::is_token_still_valid(cached) {
                    return Ok(AuthMethod::aad_token(
                        cached.access_token.as_str().to_string(),
                    ));
                }
            }
        }

        // 3. Slow Path Serialization Mutex
        let _refresh_guard = self.refresh_mutex.lock().await;

        // Double check cache
        {
            let read_guard = self.token_cache.read().await;
            if let Some(cached) = read_guard.as_ref() {
                if Self::is_token_still_valid(cached) {
                    return Ok(AuthMethod::aad_token(
                        cached.access_token.as_str().to_string(),
                    ));
                }
            }
        }

        // Fetch new token over network
        let (token, expires_in) = match self.credential_type {
            AzureCredentialType::ServicePrincipal => {
                let client_id = self
                    .get_sensitive_value("client_id")
                    .ok_or_else(|| PyValueError::new_err("Client ID not found"))?;
                let client_secret = self
                    .get_sensitive_value("client_secret")
                    .ok_or_else(|| PyValueError::new_err("Client secret not found"))?;
                let tenant_id = self
                    .get_sensitive_value("tenant_id")
                    .ok_or_else(|| PyValueError::new_err("Tenant ID not found"))?;
                self.acquire_service_principal_token(
                    client_id.as_str(),
                    client_secret.as_str(),
                    tenant_id.as_str(),
                )
                .await?
            }
            AzureCredentialType::ManagedIdentity => {
                let client_id = self.get_sensitive_value("client_id").map(|s| s.as_str());
                self.acquire_managed_identity_token(client_id).await?
            }
            AzureCredentialType::DefaultAzure => self.acquire_default_azure_token().await?,
            AzureCredentialType::AccessToken => unreachable!(),
        };

        // Enforce safety buffers against premature expiration
        let buffer_secs = ((expires_in as f64 * 0.10) as u64)
            .max(30)
            .min(600)
            .min(expires_in);
        let expires_at =
            Instant::now() + Duration::from_secs(expires_in.saturating_sub(buffer_secs));

        // Brief write lock update
        {
            let mut write_guard = self.token_cache.write().await;
            *write_guard = Some(CachedToken {
                access_token: SensitiveString::new(token.clone()),
                expires_at,
            });
        }

        // Return token string
        Ok(AuthMethod::aad_token(token))
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

        let response = self
            .client
            .post(&token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Token request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(PyRuntimeError::new_err(format!(
                "HTTP Error: {}",
                response.status()
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed parsing JSON: {}", e)))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token missing"))?
            .to_string();

        let expires_in = Self::parse_expires_in(&json, "expires_in");

        Ok((access_token, expires_in))
    }

    async fn acquire_managed_identity_token(
        &self,
        client_id: Option<&str>,
    ) -> PyResult<(String, u64)> {
        const IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
        let mut url = reqwest::Url::parse(IMDS_ENDPOINT)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid IMDS endpoint: {}", e)))?;

        url.query_pairs_mut()
            .append_pair("api-version", "2021-02-01")
            .append_pair("resource", "https://database.windows.net/");

        if let Some(id) = client_id {
            url.query_pairs_mut().append_pair("client_id", id);
        }

        let response = self
            .client
            .get(url)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("IMDS request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(PyRuntimeError::new_err(format!(
                "IMDS error status: {}",
                response.status()
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| PyRuntimeError::new_err("Access token missing"))?
            .to_string();

        let expires_in = Self::parse_expires_in(&json, "expires_in");

        Ok((access_token, expires_in))
    }

    /// Get the default Azure CLI path for the current OS
    fn get_default_az_path() -> &'static str {
        // Return bare program name on all platforms to leverage OS PATH resolution.
        // This ensures compatibility with various installation methods (Homebrew on macOS,
        // Snaps on Linux, custom installations, etc.) instead of hard-coding specific paths.
        "az"
    }

    /// Get and validate the Azure CLI path to prevent command injection
    fn get_azure_cli_path() -> PyResult<String> {
        // Try to get path from environment variable first
        let az_path = std::env::var("AZURE_CLI_PATH")
            .unwrap_or_else(|_| Self::get_default_az_path().to_string());

        // Check if this is a bare program name (no path separators).
        // If so, allow PATH resolution and skip existence validation.
        //
        // On Windows, also treat drive-relative paths like `C:az.cmd` as explicit paths
        // (they contain no separators but are still path-like).
        let path = Path::new(&az_path);
        let is_bare_name = !az_path.contains('/')
            && !az_path.contains('\\')
            && !(cfg!(windows) && az_path.contains(':'));
        if is_bare_name {
            // Bare program name - let the OS resolve it via PATH
            return Ok(az_path);
        }

        // For explicit paths, validate that the path exists and is accessible
        if !path.exists() {
            return Err(PyRuntimeError::new_err(format!(
                "Azure CLI executable not found at '{}'. Set AZURE_CLI_PATH environment variable if installed elsewhere.",
                az_path
            )));
        }

        // Verify it's a file (not a directory)
        if !path.is_file() {
            return Err(PyRuntimeError::new_err(format!(
                "Azure CLI path '{}' is not a file",
                az_path
            )));
        }

        // Check executable permission (Unix-like systems)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&az_path).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Cannot access Azure CLI at '{}': {}",
                    az_path, e
                ))
            })?;
            let permissions = metadata.permissions();
            if permissions.mode() & 0o111 == 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "Azure CLI at '{}' is not executable",
                    az_path
                )));
            }
        }

        Ok(az_path)
    }

    async fn acquire_default_azure_token(&self) -> PyResult<(String, u64)> {
        // Try service principal auth if all env vars are set and non-empty
        let client_id = std::env::var("AZURE_CLIENT_ID")
            .ok()
            .filter(|s| !s.is_empty());
        let client_secret = std::env::var("AZURE_CLIENT_SECRET")
            .ok()
            .filter(|s| !s.is_empty());
        let tenant_id = std::env::var("AZURE_TENANT_ID")
            .ok()
            .filter(|s| !s.is_empty());

        if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
            (client_id, client_secret, tenant_id)
        {
            return self
                .acquire_service_principal_token(&client_id, &client_secret, &tenant_id)
                .await;
        }

        if let Ok(res) = self.acquire_managed_identity_token(None).await {
            return Ok(res);
        }

        // Fallback to Azure CLI
        let az_path = Self::get_azure_cli_path()?;

        // Execute with validated path and timeout
        let output = match tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(&az_path)
                .args([
                    "account",
                    "get-access-token",
                    "--resource",
                    "https://database.windows.net/",
                    "--output",
                    "json",
                ])
                .output(),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Failed to execute Azure CLI from '{}': {}",
                    az_path, e
                )));
            }
            Err(_) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Azure CLI command timed out after 10 seconds (executed from '{}')",
                    az_path
                )));
            }
        };

        match output.status.success() {
            true => {
                let json: Value = serde_json::from_slice(&output.stdout)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                let access_token = json["accessToken"]
                    .as_str()
                    .ok_or_else(|| PyRuntimeError::new_err("Missing accessToken"))?
                    .to_string();

                // Azure CLI returns 'expiresOn' as an ISO 8601 timestamp string.
                // Parse it to calculate the actual token expiration duration.
                let expires_in = Self::parse_expires_on(&json, "expiresOn");

                Ok((access_token, expires_in))
            }
            false => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                Err(PyRuntimeError::new_err(format!(
                    "Azure CLI command failed (executed from '{}'): {}. Exit code: {}",
                    az_path,
                    stderr.trim(),
                    output
                        .status
                        .code()
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                )))
            }
        }
    }
}
