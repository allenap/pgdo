//! Using the [Sakila sample database][sakila] in tests.
//!
//! [sakila]: https://github.com/jOOQ/sakila

use async_std::stream::StreamExt;
use sqlx::Executor;

pub static SAKILA_SCHEMA: &str =
    include_str!("../../sakila/postgres-sakila-db/postgres-sakila-schema.sql");
pub static SAKILA_DATA: &str =
    include_str!("../../sakila/postgres-sakila-db/postgres-sakila-insert-data.sql");

/// Load the Sakila sample database into the given database.
pub async fn load_sakila(pool: &sqlx::PgPool) -> sqlx::Result<()> {
    match pool.execute("CREATE ROLE postgres").await {
        Err(err) if is_duplicate_object(&err) => (),
        Err(err) => Err(err)?,
        Ok(_) => (),
    };

    // Create schema.
    let mut stream = pool.execute_many(SAKILA_SCHEMA);
    while let Some(value) = stream.next().await {
        value?;
    }
    drop(stream);

    // Load data.
    let mut stream = pool.execute_many(SAKILA_DATA);
    while let Some(value) = stream.next().await {
        value?;
    }
    drop(stream);

    Ok(())
}

fn is_duplicate_object(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .map(|err| err.code().as_deref() == Some("42710"))
        .unwrap_or_default()
}
