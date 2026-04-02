use std::fmt;

/// Detailed errors from the SETUP process
#[derive(Debug, Clone)]
pub enum SetupError {
    // PostgreSQL
    PgConnectionFailed {
        host: String,
        error: String,
    },
    PgTableNotFound {
        table: String,
    },
    PgReplicaIdentityFailed {
        table: String,
        error: String,
    },
    PgPublicationFailed {
        name: String,
        error: String,
    },
    PgSlotFailed {
        name: String,
        error: String,
    },

    // General
    #[allow(dead_code)]
    CheckpointFailed {
        error: String,
    },
}

impl SetupError {
    /// Descriptive message for gRPC Health Check
    pub fn to_grpc_message(&self) -> String {
        match self {
            SetupError::PgConnectionFailed { host, error } => {
                format!("PostgreSQL connection failed to '{}': {}", host, error)
            }
            SetupError::PgTableNotFound { table } => {
                format!("Table '{}' not found in PostgreSQL. Verify the table exists and is accessible.", table)
            }
            SetupError::PgReplicaIdentityFailed { table, error } => {
                format!(
                    "Failed to set REPLICA IDENTITY FULL on '{}': {}",
                    table, error
                )
            }
            SetupError::PgPublicationFailed { name, error } => {
                format!("Failed to setup publication '{}': {}", name, error)
            }
            SetupError::PgSlotFailed { name, error } => {
                format!("Failed to setup replication slot '{}': {}", name, error)
            }
            SetupError::CheckpointFailed { error } => {
                format!("Checkpoint load failed: {}", error)
            }
        }
    }
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_grpc_message())
    }
}

impl std::error::Error for SetupError {}
