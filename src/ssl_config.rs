use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use std::path::PathBuf;

/// SSL/TLS configuration options for database connections
#[pyclass(name = "SslConfig")]
#[derive(Clone, Debug)]
pub struct PySslConfig {
    /// Encryption level for the connection
    pub encryption_level: EncryptionLevel,
    /// Trust server certificate without validation (dangerous in production)
    pub trust_server_certificate: bool,
    /// Path to custom CA certificate file (.pem, .crt, or .der)
    pub ca_certificate_path: Option<PathBuf>,
    /// Enable Server Name Indication (SNI)
    pub enable_sni: bool,
    /// Custom server name for certificate validation
    pub server_name: Option<String>,
}

/// Encryption levels for TLS connections
#[pyclass(name = "EncryptionLevel")]
#[derive(Clone, Debug, PartialEq)]
pub enum EncryptionLevel {
    /// All traffic is encrypted (recommended)
    Required,
    /// Only login procedure is encrypted
    LoginOnly,
    /// No encryption (not recommended)
    Off,
}

#[pymethods]
impl EncryptionLevel {
    #[classattr]
    const REQUIRED: EncryptionLevel = EncryptionLevel::Required;
    
    #[classattr]
    const LOGIN_ONLY: EncryptionLevel = EncryptionLevel::LoginOnly;
    
    #[classattr]
    const OFF: EncryptionLevel = EncryptionLevel::Off;

    pub fn __str__(&self) -> String {
        match self {
            EncryptionLevel::Required => "Required".to_string(),
            EncryptionLevel::LoginOnly => "LoginOnly".to_string(),
            EncryptionLevel::Off => "Off".to_string(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("EncryptionLevel.{}", self.__str__())
    }
}

#[pymethods]
impl PySslConfig {
    #[new]
    #[pyo3(signature = (
        encryption_level = None,
        trust_server_certificate = false,
        ca_certificate_path = None,
        enable_sni = true,
        server_name = None
    ))]
    pub fn new(
        encryption_level: Option<EncryptionLevel>,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
        enable_sni: bool,
        server_name: Option<String>,
    ) -> PyResult<Self> {
        // Validate CA certificate path if provided
        if let Some(ref path_str) = ca_certificate_path {
            let path = PathBuf::from(path_str);
            if !path.exists() {
                return Err(PyValueError::new_err(format!(
                    "CA certificate file does not exist: {}", path_str
                )));
            }
            
            // Check if the file is readable by trying to open it
            match std::fs::File::open(&path) {
                Ok(_) => {}, // File is readable, continue validation
                Err(e) => {
                    return Err(PyValueError::new_err(format!(
                        "CA certificate file is not readable: {} ({})", path_str, e
                    )));
                }
            }
            
            // Check file extension
            if let Some(ext) = path.extension() {
                let ext = ext.to_string_lossy().to_lowercase();
                if !matches!(ext.as_str(), "pem" | "crt" | "der") {
                    return Err(PyValueError::new_err(
                        "CA certificate must be .pem, .crt, or .der file"
                    ));
                }
            } else {
                return Err(PyValueError::new_err(
                    "CA certificate file must have .pem, .crt, or .der extension"
                ));
            }
        }

        // Validate trust_server_certificate and ca_certificate_path are mutually exclusive
        if trust_server_certificate && ca_certificate_path.is_some() {
            return Err(PyValueError::new_err(
                "trust_server_certificate and ca_certificate_path are mutually exclusive"
            ));
        }

        Ok(PySslConfig {
            encryption_level: encryption_level.unwrap_or(EncryptionLevel::Required),
            trust_server_certificate,
            ca_certificate_path: ca_certificate_path.map(PathBuf::from),
            enable_sni,
            server_name,
        })
    }

    /// Create SSL config for development (trusts all certificates)
    #[staticmethod]
    pub fn development() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Required,
            trust_server_certificate: true,
            ca_certificate_path: None,
            enable_sni: false,
            server_name: None,
        }
    }

    /// Create SSL config for production with custom CA certificate
    #[staticmethod]
    pub fn with_ca_certificate(ca_cert_path: String) -> PyResult<Self> {
        PySslConfig::new(
            Some(EncryptionLevel::Required),
            false,
            Some(ca_cert_path),
            true,
            None
        )
    }

    /// Create SSL config that only encrypts login (legacy mode)
    #[staticmethod]
    pub fn login_only() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::LoginOnly,
            trust_server_certificate: false,
            ca_certificate_path: None,
            enable_sni: true,
            server_name: None,
        }
    }

    /// Create SSL config with no encryption (not recommended)
    #[staticmethod]
    pub fn disabled() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Off,
            trust_server_certificate: false,
            ca_certificate_path: None,
            enable_sni: true,
            server_name: None,
        }
    }

    // Getters
    #[getter]
    pub fn encryption_level(&self) -> EncryptionLevel {
        self.encryption_level.clone()
    }

    #[getter]
    pub fn trust_server_certificate(&self) -> bool {
        self.trust_server_certificate
    }

    #[getter]
    pub fn ca_certificate_path(&self) -> Option<String> {
        self.ca_certificate_path.as_ref().map(|p| p.to_string_lossy().to_string())
    }

    #[getter]
    pub fn enable_sni(&self) -> bool {
        self.enable_sni
    }

    #[getter]
    pub fn server_name(&self) -> Option<String> {
        self.server_name.clone()
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!(
            "SslConfig(encryption={:?}, trust_cert={}, ca_cert={:?}, sni={}, server_name={:?})",
            self.encryption_level,
            self.trust_server_certificate,
            self.ca_certificate_path,
            self.enable_sni,
            self.server_name
        )
    }

    /// Representation
    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl PySslConfig {
    /// Convert to Tiberius encryption level
    pub fn to_tiberius_encryption(&self) -> tiberius::EncryptionLevel {
        match self.encryption_level {
            EncryptionLevel::Required => tiberius::EncryptionLevel::Required,
            EncryptionLevel::LoginOnly => tiberius::EncryptionLevel::On,
            EncryptionLevel::Off => tiberius::EncryptionLevel::Off,
        }
    }

    /// Apply SSL configuration to Tiberius Config
    pub fn apply_to_config(&self, config: &mut tiberius::Config) {
        // Set encryption level
        config.encryption(self.to_tiberius_encryption());

        // Configure trust settings
        if self.trust_server_certificate {
            config.trust_cert();
        } else if let Some(ref ca_path) = self.ca_certificate_path {
            config.trust_cert_ca(ca_path.to_string_lossy().to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_ssl_config_creation() {
        let ssl_config = PySslConfig::new(None, false, None, true, None).unwrap();
        assert_eq!(ssl_config.encryption_level, EncryptionLevel::Required);
        assert!(!ssl_config.trust_server_certificate);
        assert!(ssl_config.ca_certificate_path.is_none());
        assert!(ssl_config.enable_sni);
        assert!(ssl_config.server_name.is_none());
    }

    #[test]
    fn test_development_config() {
        let ssl_config = PySslConfig::development();
        assert_eq!(ssl_config.encryption_level, EncryptionLevel::Required);
        assert!(ssl_config.trust_server_certificate);
        assert!(!ssl_config.enable_sni);
    }

    #[test]
    fn test_mutual_exclusion_validation() {
        let result = PySslConfig::new(
            None,
            true, // trust_server_certificate
            Some("test.pem".to_string()), // ca_certificate_path
            true,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_ca_certificate_validation() {
        // Test with non-existent file
        let result = PySslConfig::new(
            None,
            false,
            Some("non_existent.pem".to_string()),
            true,
            None,
        );
        assert!(result.is_err());

        // Test with valid file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.pem");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "-----BEGIN CERTIFICATE-----").unwrap();
        writeln!(file, "test certificate content").unwrap();
        writeln!(file, "-----END CERTIFICATE-----").unwrap();
        
        let result = PySslConfig::new(
            None,
            false,
            Some(file_path.to_string_lossy().to_string()),
            true,
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_encryption_level_conversion() {
        let ssl_config = PySslConfig::new(
            Some(EncryptionLevel::Required),
            false,
            None,
            true,
            None,
        ).unwrap();
        
        assert_eq!(
            ssl_config.to_tiberius_encryption(),
            tiberius::EncryptionLevel::Required
        );
    }
}
