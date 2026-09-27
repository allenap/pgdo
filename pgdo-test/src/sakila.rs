//! Using the [Sakila sample database][sakila] in tests.
//!
//! [sakila]: https://github.com/jOOQ/sakila

use postgres::{error::SqlState, GenericClient};

pub static SAKILA_SCHEMA: &str =
    include_str!("../../sakila/postgres-sakila-db/postgres-sakila-schema.sql");
pub static SAKILA_DATA: &str =
    include_str!("../../sakila/postgres-sakila-db/postgres-sakila-insert-data.sql");

/// Load the Sakila sample database into the given database.
pub fn load_sakila(client: &mut impl GenericClient) -> Result<(), postgres::Error> {
    match client.batch_execute("CREATE ROLE postgres") {
        Err(err) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => (),
        Err(err) => Err(err)?,
        Ok(()) => (),
    }

    // Create schema, then load data.
    client.batch_execute(SAKILA_SCHEMA)?;
    client.batch_execute(SAKILA_DATA)?;

    Ok(())
}
