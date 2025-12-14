// Copyright 2025
// Licensed under the Elastic License v2.0

mod wal_handler;

pub use wal_handler::{WalMessage, parse_replication_message, handle_xlog_data, handle_keepalive};


