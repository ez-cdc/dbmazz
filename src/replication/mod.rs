// Copyright 2025
// Licensed under the Elastic License v2.0

mod wal_handler;

pub use wal_handler::{handle_keepalive, handle_xlog_data, parse_replication_message, WalMessage};
