// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL setup utilities.
//!
//! Currently, GTID/binlog validation is inlined in [`MysqlSource::setup()`](super::MysqlSource).
//! As the MySQL CDC implementation grows, setup-related checks (privilege validation,
//! Aurora detection, connection parameter tuning) should be extracted into this module.

// Future: extract setup checks here when they grow beyond the inline implementation.
