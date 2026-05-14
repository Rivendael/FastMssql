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
            use std::io::Read;

            let path = PathBuf::from(path_str);

            // Check file extension first — provides an early, user-friendly error message
            // before any I/O is attempted.
            if let Some(ext) = path.extension() {
                let ext = ext.to_string_lossy().to_lowercase();
                if !matches!(ext.as_str(), "pem" | "crt" | "cer" | "der") {
                    return Err(PyValueError::new_err(
                        "CA certificate must be a .pem, .crt, .cer, or .der file",
                    ));
                }
            } else {
                return Err(PyValueError::new_err(
                    "CA certificate file must have a .pem, .crt, .cer, or .der extension",
                ));
            }

            // Open the file once — this confirms existence, readability, and lets us
            // inspect the content magic bytes in a single syscall sequence.
            let mut file = std::fs::File::open(&path).map_err(|e| {
                PyValueError::new_err(format!(
                    "CA certificate file cannot be opened: {} ({})",
                    path_str, e
                ))
            })?;

            // Read the first 11 bytes to check for recognised certificate magic.
            //
            // Two formats are accepted:
            //   PEM  — starts with "-----BEGIN" (ASCII, typically "-----BEGIN CERTIFICATE-----")
            //   DER  — starts with 0x30 (ASN.1 SEQUENCE tag); all X.509 DER certs begin with it
            //
            // Extensions alone are easily bypassed by file renaming or symlinks, so this
            // content check provides defence-in-depth to catch misconfigurations early.
            //
            // Note: a TOCTOU window remains between this check and the TLS handshake
            // (when tiberius re-reads the file via trust_cert_ca).  That window is
            // inherent to tiberius's path-string API and cannot be fully closed here.
            let mut magic = [0u8; 11];
            let n = file.read(&mut magic).map_err(|e| {
                PyValueError::new_err(format!(
                    "Failed to read CA certificate file: {} ({})",
                    path_str, e
                ))
            })?;

            let is_pem = n >= 10 && magic[..10] == *b"-----BEGIN";
            // ASN.1 DER SEQUENCE (0x30) — all well-formed X.509 certificates start with this.
            let is_der = n >= 1 && magic[0] == 0x30;

            if !is_pem && !is_der {
                return Err(PyValueError::new_err(format!(
                    "CA certificate file does not contain valid PEM or DER certificate data: {}. \
                     PEM files must start with '-----BEGIN …'; DER files are binary ASN.1 starting \
                     with byte 0x30. Ensure the file is a CA certificate, not a private key or \
                     other credential.",
                    path_str
                )));
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
