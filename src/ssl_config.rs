use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::io::Read;
use std::path::PathBuf;

/// Encryption levels for TLS connections
// Added from_py_object here to opt into the modern PyO3 behavior cleanly
#[pyclass(name = "EncryptionLevel", eq, eq_int, from_py_object)]
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

/// SSL/TLS configuration options for database connections
#[pyclass(name = "SslConfig", from_py_object)]
#[derive(Clone, Debug)]
pub struct PySslConfig {
    pub encryption_level: EncryptionLevel,
    pub trust_server_certificate: bool,
    pub ca_certificate_path: Option<PathBuf>,
}

impl PySslConfig {
    /// Internal validation utility pipeline
    fn validate_and_build(
        encryption_level: EncryptionLevel,
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
    ) -> PyResult<Self> {
        if trust_server_certificate && ca_certificate_path.is_some() {
            return Err(PyValueError::new_err(
                "trust_server_certificate and ca_certificate_path are mutually exclusive",
            ));
        }

        let path_buf = if let Some(path_str) = ca_certificate_path {
            let path = PathBuf::from(path_str);

            match path.extension().map(|e| e.to_string_lossy().to_lowercase()) {
                Some(ref ext) if matches!(ext.as_str(), "pem" | "crt" | "cer" | "der") => {}
                _ => return Err(PyValueError::new_err(
                    "CA certificate file must have a .pem, .crt, .cer, or .der extension",
                )),
            }

            let mut file = std::fs::File::open(&path).map_err(|e| {
                PyValueError::new_err(format!("CA certificate file cannot be opened: {}", e))
            })?;

            let mut magic = [0u8; 11];
            let bytes_read = file.read(&mut magic).map_err(|e| {
                PyValueError::new_err(format!("Failed to read CA certificate file: {}", e))
            })?;

            let is_pem = bytes_read >= 10 && &magic[..10] == b"-----BEGIN";
            let is_der = bytes_read >= 1 && magic[0] == 0x30;

            if !is_pem && !is_der {
                return Err(PyValueError::new_err(
                    "CA certificate file does not contain valid PEM or DER certificate data."
                ));
            }
            Some(path)
        } else {
            None
        };

        Ok(PySslConfig {
            encryption_level,
            trust_server_certificate,
            ca_certificate_path: path_buf,
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
        encryption_level: Option<&Bound<'_, PyAny>>, // Extracted manually inside constructor to stay clean
        trust_server_certificate: bool,
        ca_certificate_path: Option<String>,
    ) -> PyResult<Self> {
        let level = match encryption_level {
            Some(bound) => {
                if let Ok(enum_val) = bound.extract::<EncryptionLevel>() {
                    enum_val
                } else if let Ok(level_str) = bound.extract::<String>() {
                    match level_str.to_lowercase().replace('_', "").as_str() {
                        "required" => EncryptionLevel::Required,
                        "loginonly" => EncryptionLevel::LoginOnly,
                        "off" | "disabled" => EncryptionLevel::Disabled,
                        _ => return Err(PyValueError::new_err(format!(
                            "Invalid encryption level '{}'. Choose from 'Required', 'LoginOnly', or 'Disabled'",
                            level_str
                        ))),
                    }
                } else {
                    return Err(PyValueError::new_err(
                        "encryption_level must be a string or an EncryptionLevel enum"
                    ));
                }
            }
            None => EncryptionLevel::Required,
        };

        Self::validate_and_build(level, trust_server_certificate, ca_certificate_path)
    }

    #[staticmethod]
    pub fn development() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Required,
            trust_server_certificate: true,
            ca_certificate_path: None,
        }
    }

    #[staticmethod]
    pub fn with_ca_certificate(ca_cert_path: String) -> PyResult<Self> {
        Self::validate_and_build(EncryptionLevel::Required, false, Some(ca_cert_path))
    }

    #[staticmethod]
    pub fn login_only() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::LoginOnly,
            trust_server_certificate: false,
            ca_certificate_path: None,
        }
    }

    #[staticmethod]
    pub fn disabled() -> Self {
        PySslConfig {
            encryption_level: EncryptionLevel::Disabled,
            trust_server_certificate: false,
            ca_certificate_path: None,
        }
    }

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
        self.ca_certificate_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
    }

    pub fn __str__(&self) -> String {
        format!(
            "SslConfig(encryption={:?}, trust_cert={}, ca_cert={:?})",
            self.encryption_level, self.trust_server_certificate, self.ca_certificate_path,
        )
    }

    pub fn __repr__(&self) -> String {
        format!(
            "SslConfig(encryption_level={}, trust_server_certificate={}, ca_certificate_path={:?})",
            self.encryption_level.__str__(),
            self.trust_server_certificate,
            self.ca_certificate_path()
        )
    }

    pub fn __eq__(&self, other: &PySslConfig) -> bool {
        self.encryption_level == other.encryption_level
            && self.trust_server_certificate == other.trust_server_certificate
            && self.ca_certificate_path == other.ca_certificate_path
    }
}

impl PySslConfig {
    pub fn to_tiberius_encryption(&self) -> tiberius::EncryptionLevel {
        match self.encryption_level {
            EncryptionLevel::Required => tiberius::EncryptionLevel::Required,
            EncryptionLevel::LoginOnly => tiberius::EncryptionLevel::Off,
            EncryptionLevel::Disabled => tiberius::EncryptionLevel::NotSupported,
        }
    }

    pub fn apply_to_config(&self, config: &mut tiberius::Config) {
        config.encryption(self.to_tiberius_encryption());
        if self.trust_server_certificate {
            config.trust_cert();
        } else if let Some(ca_path) = &self.ca_certificate_path {
            config.trust_cert_ca(ca_path.to_string_lossy().to_string());
        }
    }
}