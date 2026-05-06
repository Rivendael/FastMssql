use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::PathBuf;

/// SSL/TLS configuration options for database connections
#[pyclass(name = "SslConfig", from_py_object)]
#[derive(Clone, Debug)]
pub struct PySslConfig {
    /// Encryption level for the connection
    pub encryption_level: EncryptionLevel,
    /// Trust server certificate without validation (dangerous in production)
    pub trust_server_certificate: bool,
    /// Path to custom CA certificate file (.pem, .crt, or .der)
    pub ca_certificate_path: Option<PathBuf>,
}

/// Encryption levels for TLS connections
#[pyclass(name = "EncryptionLevel", from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub enum EncryptionLevel {
    /// All traffic is encrypted (recommended)
    Required,
    /// Only login procedure is encrypted
    LoginOnly,
    /// No encryption (not recommended)
    Disabled,
}

#[pymethods]
impl EncryptionLevel {
    #[classattr]
    const REQUIRED: EncryptionLevel = EncryptionLevel::Required;

    #[classattr]
    const LOGIN_ONLY: EncryptionLevel = EncryptionLevel::LoginOnly;

    #[classattr]
    const DISABLED: EncryptionLevel = EncryptionLevel::Disabled;

    pub fn __str__(&self) -> String {
        match self {
            EncryptionLevel::Required => "Required".into(),
            EncryptionLevel::LoginOnly => "LoginOnly".into(),
            EncryptionLevel::Disabled => "Disabled".into(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("EncryptionLevel.{}", self.__str__())
    }
}

/// Helper function to convert string to EncryptionLevel
fn parse_encryption_level(level: &str) -> PyResult<EncryptionLevel> {
    match level.to_lowercase().as_str() {
        "required" => Ok(EncryptionLevel::Required),
        "loginonly" | "login_only" => Ok(EncryptionLevel::LoginOnly),
        "off" | "disabled" => Ok(EncryptionLevel::Disabled),
        _ => Err(PyValueError::new_err(format!(
            "Invalid encryption_level '{}'. Valid values are: 'Required', 'LoginOnly', or 'Disabled'",
            level
        ))),
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
                "trust_server_certificate and ca_certificate_path are mutually exclusive",
            ));
        }

        // Validate CA certificate path if provided
        if let Some(path_str) = ca_certificate_path {
            let path = PathBuf::from(path_str);
            if !path.exists() {
                return Err(PyValueError::new_err(format!(
                    "CA certificate file does not exist: {}",
                    path_str
                )));
            }

            // Check if the file is readable by trying to open it
            match std::fs::File::open(&path) {
                Ok(_) => {} // File is readable, continue validation
                Err(e) => {
                    return Err(PyValueError::new_err(format!(
                        "CA certificate file is not readable: {} ({})",
                        path_str, e
                    )));
                }
            }

            // Check file extension
            if let Some(ext) = path.extension() {
                let ext = ext.to_string_lossy().to_lowercase();
                if !matches!(ext.as_str(), "pem" | "crt" | "cer" | "der") {
                    return Err(PyValueError::new_err(
                        "CA certificate must be .pem, .crt, .cer, or .der file",
                    ));
                }
            } else {
                return Err(PyValueError::new_err(
                    "CA certificate file must have .pem, .crt, .cer, or .der extension",
                ));
            }
        }

        Ok(())
    }



    /// Internal constructor for Rust code
    pub fn new_internal(
        encryption_level: EncryptionLevel,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
    ) -> PyResult<Self> {
        Self::validate_certificate_config(trust_server_certificate, &ca_certificate_path)?;

        Ok(PySslConfig {
            encryption_level,
            trust_server_certificate,
            ca_certificate_path: ca_certificate_path.map(PathBuf::from),
        })
    }
}

#[pymethods]
impl PySslConfig {
    #[new]
    #[pyo3(signature = (
        encryption_level = None,
        trust_server_certificate = false,
        ca_certificate_path = None
    ))]
    pub fn new(
        encryption_level: Option<&Bound<PyAny>>,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
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
                return Err(PyValueError::new_err(
                    "encryption_level must be a string or EncryptionLevel enum",
                ));
            }
        } else {
            EncryptionLevel::Required // Default value
        };

        Self::validate_certificate_config(trust_server_certificate, &ca_certificate_path)?;

        Ok(PySslConfig {
            encryption_level,
            trust_server_certificate,
            ca_certificate_path: ca_certificate_path.map(PathBuf::from),
        })
    }

    /// Create SSL config for development (trusts all certificates)
    #[staticmethod]
    pub fn development() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Required,
            trust_server_certificate: true,
            ca_certificate_path: None,
        }
    }

    /// Create SSL config for production with custom CA certificate
    #[staticmethod]
    pub fn with_ca_certificate(ca_cert_path: String) -> PyResult<Self> {
        PySslConfig::new_internal(
            EncryptionLevel::Required,
            false,
            Some(ca_cert_path),
        )
    }

    /// Create SSL config that only encrypts login (legacy mode)
    #[staticmethod]
    pub fn login_only() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::LoginOnly,
            trust_server_certificate: false,
            ca_certificate_path: None,
        }
    }

    /// Create SSL config with no encryption (not recommended)
    #[staticmethod]
    pub fn disabled() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Disabled,
            trust_server_certificate: false,
            ca_certificate_path: None,
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
        self.ca_certificate_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!(
            "SslConfig(encryption={:?}, trust_cert={}, ca_cert={:?})",
            self.encryption_level,
            self.trust_server_certificate,
            self.ca_certificate_path,
        )
    }

    /// Representation
    pub fn __repr__(&self) -> String {
        format!(
            "SslConfig(encryption_level={:?}, trust_server_certificate={}, ca_certificate_path={:?})",
            self.encryption_level.__str__(),
            self.trust_server_certificate,
            self.ca_certificate_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
        )
    }

    /// Equality comparison
    pub fn __eq__(&self, other: &PySslConfig) -> bool {
        self.encryption_level == other.encryption_level
            && self.trust_server_certificate == other.trust_server_certificate
            && self.ca_certificate_path == other.ca_certificate_path
    }
}

impl PySslConfig {
    /// Convert to Tiberius encryption level
    pub fn to_tiberius_encryption(&self) -> tiberius::EncryptionLevel {
        match self.encryption_level {
            EncryptionLevel::Required => tiberius::EncryptionLevel::Required,
            // Off = login packet only encrypted; NotSupported = no encryption at all
            EncryptionLevel::LoginOnly => tiberius::EncryptionLevel::Off,
            EncryptionLevel::Disabled => tiberius::EncryptionLevel::NotSupported,
        }
    }

    /// Apply SSL configuration to Tiberius Config
    pub fn apply_to_config(&self, config: &mut tiberius::Config) {
        // Set encryption level
        config.encryption(self.to_tiberius_encryption());

        // Configure trust settings
        if self.trust_server_certificate {
            config.trust_cert();
        } else if let Some(ca_path) = &self.ca_certificate_path {
            config.trust_cert_ca(ca_path.to_string_lossy().to_string());
        }
    }
}
