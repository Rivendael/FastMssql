use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use tiberius::AuthMethod;

/// Azure credential configuration for database connections
#[pyclass(name = "AzureCredential")]
#[derive(Clone, Debug)]
pub struct PyAzureCredential {
    pub credential_type: AzureCredentialType,
    pub config: HashMap<String, String>,
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
        }
    }

    #[staticmethod]
    pub fn access_token(token: String) -> Self {
        let mut config = HashMap::new();
        config.insert("access_token".to_string(), token);

        PyAzureCredential {
            credential_type: AzureCredentialType::AccessToken,
            config,
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        PyAzureCredential {
            credential_type: AzureCredentialType::DefaultAzure,
            config: HashMap::new(),
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
        match self.credential_type {
            AzureCredentialType::AccessToken => {
                let token = self.config.get("access_token")
                    .ok_or_else(|| PyValueError::new_err("Access token not found in configuration"))?;
                Ok(AuthMethod::aad_token(token))
            }
            AzureCredentialType::ServicePrincipal => {
                let client_id = self.config.get("client_id")
                    .ok_or_else(|| PyValueError::new_err("Client ID not found in configuration"))?;
                let client_secret = self.config.get("client_secret")
                    .ok_or_else(|| PyValueError::new_err("Client secret not found in configuration"))?;
                let tenant_id = self.config.get("tenant_id")
                    .ok_or_else(|| PyValueError::new_err("Tenant ID not found in configuration"))?;

                let token = self.acquire_service_principal_token(client_id, client_secret, tenant_id).await?;
                Ok(AuthMethod::aad_token(token))
            }
            AzureCredentialType::ManagedIdentity => {
                let client_id = self.config.get("client_id");
                let token = self.acquire_managed_identity_token(client_id.cloned()).await?;
                Ok(AuthMethod::aad_token(token))
            }
            AzureCredentialType::DefaultAzure => {
                let token = self.acquire_default_azure_token().await?;
                Ok(AuthMethod::aad_token(token))
            }
        }
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