use std::fmt;

/// Common error types for the CDC core module
#[derive(Debug)]
pub enum CoreError {
    /// Source-related errors
    SourceError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Sink-related errors
    SinkError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Connection errors
    ConnectionError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Configuration errors
    ConfigError {
        message: String,
    },

    /// Serialization/deserialization errors
    SerializationError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Validation errors
    ValidationError {
        message: String,
    },

    /// Position tracking errors
    PositionError {
        message: String,
    },

    /// Transaction errors
    TransactionError {
        message: String,
    },

    /// Schema-related errors
    SchemaError {
        message: String,
    },

    /// Generic internal errors
    InternalError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

#[allow(dead_code)]
impl CoreError {
    pub fn source_error(message: impl Into<String>) -> Self {
        Self::SourceError {
            message: message.into(),
            source: None,
        }
    }

    pub fn source_error_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::SourceError {
            message: message.into(),
            source: Some(Box::new(cause)),
        }
    }

    pub fn sink_error(message: impl Into<String>) -> Self {
        Self::SinkError {
            message: message.into(),
            source: None,
        }
    }

    pub fn sink_error_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::SinkError {
            message: message.into(),
            source: Some(Box::new(cause)),
        }
    }

    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::ConnectionError {
            message: message.into(),
            source: None,
        }
    }

    pub fn connection_error_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::ConnectionError {
            message: message.into(),
            source: Some(Box::new(cause)),
        }
    }

    pub fn config_error(message: impl Into<String>) -> Self {
        Self::ConfigError {
            message: message.into(),
        }
    }

    pub fn validation_error(message: impl Into<String>) -> Self {
        Self::ValidationError {
            message: message.into(),
        }
    }

    pub fn position_error(message: impl Into<String>) -> Self {
        Self::PositionError {
            message: message.into(),
        }
    }

    pub fn transaction_error(message: impl Into<String>) -> Self {
        Self::TransactionError {
            message: message.into(),
        }
    }

    pub fn schema_error(message: impl Into<String>) -> Self {
        Self::SchemaError {
            message: message.into(),
        }
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::InternalError {
            message: message.into(),
            source: None,
        }
    }

    pub fn internal_error_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::InternalError {
            message: message.into(),
            source: Some(Box::new(cause)),
        }
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoreError::SourceError { message, source } => {
                write!(f, "Source error: {}", message)?;
                if let Some(src) = source {
                    write!(f, " (caused by: {})", src)?;
                }
                Ok(())
            }
            CoreError::SinkError { message, source } => {
                write!(f, "Sink error: {}", message)?;
                if let Some(src) = source {
                    write!(f, " (caused by: {})", src)?;
                }
                Ok(())
            }
            CoreError::ConnectionError { message, source } => {
                write!(f, "Connection error: {}", message)?;
                if let Some(src) = source {
                    write!(f, " (caused by: {})", src)?;
                }
                Ok(())
            }
            CoreError::ConfigError { message } => write!(f, "Configuration error: {}", message),
            CoreError::SerializationError { message, source } => {
                write!(f, "Serialization error: {}", message)?;
                if let Some(src) = source {
                    write!(f, " (caused by: {})", src)?;
                }
                Ok(())
            }
            CoreError::ValidationError { message } => write!(f, "Validation error: {}", message),
            CoreError::PositionError { message } => write!(f, "Position error: {}", message),
            CoreError::TransactionError { message } => write!(f, "Transaction error: {}", message),
            CoreError::SchemaError { message } => write!(f, "Schema error: {}", message),
            CoreError::InternalError { message, source } => {
                write!(f, "Internal error: {}", message)?;
                if let Some(src) = source {
                    write!(f, " (caused by: {})", src)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for CoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CoreError::SourceError { source, .. }
            | CoreError::SinkError { source, .. }
            | CoreError::ConnectionError { source, .. }
            | CoreError::SerializationError { source, .. }
            | CoreError::InternalError { source, .. } => {
                source.as_ref().map(|e| e.as_ref() as &dyn std::error::Error)
            }
            _ => None,
        }
    }
}

/// Result type alias using CoreError
pub type CoreResult<T> = Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_error_creation() {
        let err = CoreError::source_error("test error");
        assert!(err.to_string().contains("test error"));

        let err = CoreError::config_error("invalid config");
        assert!(err.to_string().contains("invalid config"));

        let err = CoreError::validation_error("validation failed");
        assert!(err.to_string().contains("validation failed"));
    }

    #[test]
    fn test_error_with_cause() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = CoreError::source_error_with_cause("failed to read file", io_err);
        assert!(err.to_string().contains("failed to read file"));
        assert!(err.source().is_some());
    }
}
