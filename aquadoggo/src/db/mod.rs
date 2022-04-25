// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{Error, Result};
use sqlx::migrate;
use sqlx::migrate::MigrateDatabase;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Sqlite;

pub mod custom_decode;
pub mod models;

pub type Pool = SqlitePool;

/// Create database when not existing.
pub async fn create_database(url: &str) -> Result<()> {
    if !Sqlite::database_exists(url).await? {
        Sqlite::create_database(url).await?;
    }

    Sqlite::drop_database(url);

    Ok(())
}

/// Create a database connection pool for postgres server.
#[cfg(not(any(feature = "mysql", feature = "sqlite")))]
pub async fn connection_pool(url: &str, max_connections: u32) -> Result<Pool, Error> {
    let pool: Pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;

    Ok(pool)
}

/// Run any pending database migrations from inside the application.
pub async fn run_pending_migrations(pool: &Pool) -> Result<()> {
    migrate!().run(pool).await?;
    Ok(())
}
