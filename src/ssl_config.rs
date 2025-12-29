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

/// Helper function to convert string to EncryptionLevel
fn parse_encryption_level(level: &str) -> PyResult<EncryptionLevel> {
    match level {
        "Required" => Ok(EncryptionLevel::Required),
        "LoginOnly" => Ok(EncryptionLevel::LoginOnly),
        "Off" => Ok(EncryptionLevel::Off),
        _ => Err(PyValueError::new_err(format!("Invalid encryption_level '{}'. Valid values are: 'Required', 'LoginOnly', 'Off'", level)))
    }
}

impl PySslConfig {
    /// Validate certificate configuration
    fn validate_certificate_config(
        trust_server_certificate: bool,
        ca_certificate_path: &Option<String>,
    ) -> PyResult<()> {
        // Validate trust_server_certificate and ca_certificate_path are mutually exclusive
        if trust_server_certificate && ca_certificate_path.is_some() {
            return Err(PyValueError::new_err(
                "trust_server_certificate and ca_certificate_path are mutually exclusive"
            ));
        }

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

        Ok(())
    }

    /// Validate encryption level configuration
    fn validate_encryption_config(
        encryption_level: EncryptionLevel,
        trust_server_certificate: bool,
        ca_certificate_path: &Option<String>,
    ) -> PyResult<()> {
        // For Required and LoginOnly encryption, ensure we have trust settings
        match encryption_level {
            EncryptionLevel::Required | EncryptionLevel::LoginOnly => {
                if !trust_server_certificate && ca_certificate_path.is_none() {
                    return Err(PyValueError::new_err(
                        "Encryption level Required or LoginOnly requires either trust_server_certificate=True or a ca_certificate_path"
                    ));
                }
            }
            EncryptionLevel::Off => {
                // No encryption, no restrictions on trust settings
            }
        }
        Ok(())
    }

    /// Internal constructor for Rust code
    pub fn new_internal(
        encryption_level: EncryptionLevel,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
        enable_sni: bool,
        server_name: Option<String>,
    ) -> PyResult<Self> {
        Self::validate_certificate_config(trust_server_certificate, &ca_certificate_path)?;
        Self::validate_encryption_config(encryption_level.clone(), trust_server_certificate, &ca_certificate_path)?;

        Ok(PySslConfig {
            encryption_level,
            trust_server_certificate,
            ca_certificate_path: ca_certificate_path.map(PathBuf::from),
            enable_sni,
            server_name,
        })
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
        encryption_level: Option<&Bound<PyAny>>,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
        enable_sni: bool,
        server_name: Option<String>,
    ) -> PyResult<Self> {
        // Handle encryption_level which can be either string or EncryptionLevel enum
        let encryption_level = if let Some(level) = encryption_level {
            if let Ok(level_str) = level.extract::<String>() {
                // String input - convert to enum
                parse_encryption_level(&level_str)?
            } else if let Ok(level_enum) = level.extract::<EncryptionLevel>() {
                // Already an enum
                level_enum
            } else {
                return Err(PyValueError::new_err("encryption_level must be a string or EncryptionLevel enum"));
            }
        } else {
            EncryptionLevel::Required // Default value
        };
        
        Self::validate_certificate_config(trust_server_certificate, &ca_certificate_path)?;
        Self::validate_encryption_config(encryption_level.clone(), trust_server_certificate, &ca_certificate_path)?;

        Ok(PySslConfig {
            encryption_level,
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
        PySslConfig::new_internal(
            EncryptionLevel::Required,
            false,
            Some(ca_cert_path),
            true,
            None
        )
    }

    /// Create SSL config that only encrypts login (legacy mode)
    /// Note: This trusts server certificates for compatibility
    #[staticmethod]
    pub fn login_only() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::LoginOnly,
            trust_server_certificate: true,
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
    pub fn encryption_level(&self) -> String {
        self.encryption_level.__str__()
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

        // Note: SNI (Server Name Indication) support in Tiberius is handled automatically
        // when using rustls. The TLS handshake will use the hostname from the connection
        // string. The 'server_name' field can be used for custom certificate validation
        // if needed in future versions, but currently Tiberius handles this internally.
        // The enable_sni field is preserved for API completeness and future extensibility.
    }
}