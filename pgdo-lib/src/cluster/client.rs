//! A minimal, synchronous PostgreSQL client.
//!
//! This is just enough client for pgdo's own needs: it connects over a
//! Unix-domain socket, supports only [trust authentication][trust] – which is
//! how pgdo creates clusters – and sends queries using the [simple query
//! protocol][simple-query], receiving all values as text.
//!
//! It is synchronous in the most ordinary way: it blocks on socket I/O. Unlike
//! clients that wrap an async implementation, it is safe to call from within an
//! async context, though it will block the calling thread.
//!
//! To work with a cluster from your own code, use a full-featured client
//! library, connecting via [`Cluster::datadir`][`super::Cluster::datadir`]
//! or [`Cluster::url`][`super::Cluster::url`].
//!
//! [trust]: https://www.postgresql.org/docs/current/auth-trust.html
//! [simple-query]:
//!     https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY

use std::fmt;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::{backend::Message, frontend};

/// The port that pgdo's clusters use. This determines the name of the socket.
const PORT: u16 = 5432;

/// SQLSTATE for "duplicate database".
pub(crate) const DUPLICATE_DATABASE: &str = "42P04";
/// SQLSTATE for "undefined database" (a.k.a. "invalid catalog name").
pub(crate) const UNDEFINED_DATABASE: &str = "3D000";

/// Error communicating with a cluster.
#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum ClientError {
    #[error("Could not communicate with cluster")]
    IoError(#[from] io::Error),
    #[error(transparent)]
    #[diagnostic(transparent)]
    ServerError(Box<ServerError>),
    #[error("Cluster requires {0} authentication, which pgdo does not support")]
    #[diagnostic(help(
        "pgdo creates clusters with trust authentication; check `pg_hba.conf` in the cluster"
    ))]
    AuthenticationError(&'static str),
    #[error("Unexpected message from cluster: {0}")]
    ProtocolError(String),
}

impl From<ServerError> for ClientError {
    fn from(error: ServerError) -> Self {
        Self::ServerError(Box::new(error))
    }
}

/// An error reported by the PostgreSQL server.
#[derive(Debug, Clone)]
pub struct ServerError {
    /// e.g. `ERROR`, `FATAL`.
    pub severity: String,
    /// The [SQLSTATE code][sqlstate], e.g. `42P04`.
    ///
    /// [sqlstate]: https://www.postgresql.org/docs/current/errcodes-appendix.html
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl std::error::Error for ServerError {}

impl miette::Diagnostic for ServerError {
    fn help<'a>(&'a self) -> Option<Box<dyn fmt::Display + 'a>> {
        self.hint
            .as_ref()
            .map(|hint| Box::new(hint) as Box<dyn fmt::Display>)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} (SQLSTATE {})",
            self.severity, self.message, self.code
        )?;
        if let Some(detail) = &self.detail {
            write!(f, "; {detail}")?;
        }
        Ok(())
    }
}

impl ServerError {
    fn from_fields(body: &postgres_protocol::message::backend::ErrorResponseBody) -> Self {
        let mut error = ServerError {
            severity: String::new(),
            code: String::new(),
            message: String::new(),
            detail: None,
            hint: None,
        };
        let mut fields = body.fields();
        // Stop at the first malformed field; we'll use what we have so far.
        while let Ok(Some(field)) = fields.next() {
            let value = String::from_utf8_lossy(field.value_bytes()).into_owned();
            match field.type_() {
                // Non-localised severity is preferred; it's only present in
                // PostgreSQL 9.6 and later, but that's everything we support.
                b'V' => error.severity = value,
                b'C' => error.code = value,
                b'M' => error.message = value,
                b'D' => error.detail = Some(value),
                b'H' => error.hint = Some(value),
                _ => (),
            }
        }
        error
    }
}

/// A row returned from a query. All values are text, or `None` for `NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row(Vec<Option<String>>);

impl Row {
    /// Get the value in the given column. Panics if the column is out of
    /// range; that's a programming error.
    pub fn get(&self, column: usize) -> Option<&str> {
        self.0[column].as_deref()
    }
}

/// A connection to a cluster.
pub(crate) struct Client {
    stream: UnixStream,
    buf: BytesMut,
}

impl Client {
    /// Connect to the cluster listening in `socketdir`, with trust
    /// authentication.
    pub fn connect(socketdir: &Path, user: &str, database: &str) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socketdir.join(format!(".s.PGSQL.{PORT}")))?;
        let mut client = Client { stream, buf: BytesMut::new() };

        let mut message = BytesMut::new();
        frontend::startup_message(
            [
                ("user", user),
                ("database", database),
                ("application_name", "pgdo"),
                // We decode values as UTF-8; ask the server to send them so.
                ("client_encoding", "UTF8"),
            ],
            &mut message,
        )?;
        client.stream.write_all(&message)?;

        // Authentication.
        match client.receive()? {
            Message::AuthenticationOk => (),
            Message::ErrorResponse(body) => Err(ServerError::from_fields(&body))?,
            Message::AuthenticationCleartextPassword => {
                Err(ClientError::AuthenticationError("password"))?;
            }
            Message::AuthenticationMd5Password(_) => Err(ClientError::AuthenticationError("MD5"))?,
            Message::AuthenticationSasl(_) => Err(ClientError::AuthenticationError("SASL"))?,
            Message::AuthenticationGss | Message::AuthenticationSspi => {
                Err(ClientError::AuthenticationError("GSSAPI/SSPI"))?;
            }
            message => Err(unexpected(&message))?,
        }

        // Wait for the server to be ready, ignoring parameter statuses, etc.
        loop {
            match client.receive()? {
                Message::ReadyForQuery(_) => break Ok(client),
                Message::ErrorResponse(body) => Err(ServerError::from_fields(&body))?,
                Message::ParameterStatus(_)
                | Message::BackendKeyData(_)
                | Message::NoticeResponse(_) => (),
                message => Err(unexpected(&message))?,
            }
        }
    }

    /// Execute the given SQL and return all rows from all statements.
    ///
    /// This uses the simple query protocol, so `sql` may contain several
    /// statements, but cannot have parameters: escape values into it with
    /// [`postgres_protocol::escape`]. `COPY` is not supported.
    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>, ClientError> {
        let mut message = BytesMut::new();
        frontend::query(sql, &mut message)?;
        self.stream.write_all(&message)?;

        let mut rows = Vec::new();
        let mut error = None;
        loop {
            match self.receive()? {
                Message::ReadyForQuery(_) => break,
                Message::DataRow(body) => {
                    let buffer = body.buffer();
                    let row = body
                        .ranges()
                        .map(|range| {
                            Ok(range
                                .map(|range| String::from_utf8_lossy(&buffer[range]).into_owned()))
                        })
                        .collect()?;
                    rows.push(Row(row));
                }
                Message::ErrorResponse(body) => error = Some(ServerError::from_fields(&body)),
                Message::RowDescription(_)
                | Message::CommandComplete(_)
                | Message::EmptyQueryResponse
                | Message::NoticeResponse(_)
                | Message::ParameterStatus(_) => (),
                message => Err(unexpected(&message))?,
            }
        }

        match error {
            Some(error) => Err(error)?,
            None => Ok(rows),
        }
    }

    /// Execute the given SQL, discarding any rows.
    pub fn execute(&mut self, sql: &str) -> Result<(), ClientError> {
        self.query(sql).map(|_| ())
    }

    /// Receive one message from the server.
    fn receive(&mut self) -> Result<Message, ClientError> {
        loop {
            if let Some(message) = Message::parse(&mut self.buf)? {
                return Ok(message);
            }
            let mut chunk = [0u8; 8192];
            match self.stream.read(&mut chunk)? {
                0 => Err(io::Error::from(io::ErrorKind::UnexpectedEof))?,
                n => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Say goodbye politely. If this fails the server will notice the
        // connection has gone anyway.
        let mut message = BytesMut::new();
        frontend::terminate(&mut message);
        let _ = self.stream.write_all(&message);
    }
}

fn unexpected(message: &Message) -> ClientError {
    // `Message` does not implement `Debug`, so describe it by its variant.
    let name = match message {
        Message::CopyInResponse(_) | Message::CopyOutResponse(_) | Message::CopyData(_) => "COPY",
        Message::NotificationResponse(_) => "notification",
        Message::AuthenticationSaslContinue(_) | Message::AuthenticationSaslFinal(_) => {
            "authentication"
        }
        _ => "other",
    };
    ClientError::ProtocolError(name.into())
}

#[cfg(test)]
mod tests {
    use super::{Client, ClientError, Row, UNDEFINED_DATABASE};
    use crate::cluster::{Cluster, DATABASE_POSTGRES};
    use crate::runtime::strategy::Strategy;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn with_cluster(test: impl FnOnce(&Cluster) -> TestResult) -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let cluster = Cluster::new(tempdir.path().join("data"), Strategy::default())?;
        cluster.start(&[])?;
        let result = test(&cluster);
        cluster.stop()?;
        result
    }

    fn connect(cluster: &Cluster, database: &str) -> Result<Client, ClientError> {
        let user = crate::util::current_user().unwrap();
        Client::connect(&cluster.datadir, &user, database)
    }

    #[test]
    fn query_returns_rows_including_nulls() -> TestResult {
        with_cluster(|cluster| {
            let mut client = connect(cluster, DATABASE_POSTGRES)?;
            let rows = client.query("SELECT 1, NULL, 'three' UNION ALL SELECT 4, '5', NULL")?;
            assert_eq!(
                rows,
                vec![
                    Row(vec![Some("1".into()), None, Some("three".into())]),
                    Row(vec![Some("4".into()), Some("5".into()), None]),
                ]
            );
            Ok(())
        })
    }

    #[test]
    fn query_returns_rows_from_all_statements() -> TestResult {
        with_cluster(|cluster| {
            let mut client = connect(cluster, DATABASE_POSTGRES)?;
            let rows = client.query("SELECT 1; SELECT 2; ; SET application_name = 'foo'")?;
            let values: Vec<_> = rows.iter().map(|row| row.get(0)).collect();
            assert_eq!(values, vec![Some("1"), Some("2")]);
            Ok(())
        })
    }

    #[test]
    fn query_returns_large_results() -> TestResult {
        with_cluster(|cluster| {
            let mut client = connect(cluster, DATABASE_POSTGRES)?;
            // Larger than the read buffer, to exercise reassembly of messages.
            let rows = client.query("SELECT repeat('x', 100000) FROM generate_series(1, 3)")?;
            assert_eq!(rows.len(), 3);
            assert!(rows
                .iter()
                .all(|row| row.get(0).map(str::len) == Some(100_000)));
            Ok(())
        })
    }

    #[test]
    fn query_reports_server_errors_and_client_remains_usable() -> TestResult {
        with_cluster(|cluster| {
            let mut client = connect(cluster, DATABASE_POSTGRES)?;
            match client.query("SELECT * FROM no_such_table") {
                Err(ClientError::ServerError(error)) => {
                    assert_eq!(error.severity, "ERROR");
                    assert_eq!(error.code, "42P01"); // undefined_table.
                }
                other => panic!("unexpected result: {other:?}"),
            }
            assert_eq!(client.query("SELECT 'ok'")?[0].get(0), Some("ok"));
            Ok(())
        })
    }

    #[test]
    fn query_decodes_utf8() -> TestResult {
        with_cluster(|cluster| {
            let mut client = connect(cluster, DATABASE_POSTGRES)?;
            let rows = client.query("SELECT 'Hello, 世界 🐘'")?;
            assert_eq!(rows[0].get(0), Some("Hello, 世界 🐘"));
            Ok(())
        })
    }

    #[test]
    fn connect_reports_missing_database() -> TestResult {
        with_cluster(|cluster| {
            match connect(cluster, "no-such-database") {
                Err(ClientError::ServerError(error)) => {
                    assert_eq!(error.severity, "FATAL");
                    assert_eq!(error.code, UNDEFINED_DATABASE);
                }
                Err(err) => panic!("unexpected error: {err:?}"),
                Ok(_) => panic!("unexpectedly connected"),
            }
            Ok(())
        })
    }

    #[test]
    fn client_works_within_async_context() -> TestResult {
        // The `postgres` crate panics here; this client merely blocks.
        with_cluster(|cluster| {
            let runtime = tokio::runtime::Builder::new_current_thread().build()?;
            runtime.block_on(async {
                let mut client = connect(cluster, DATABASE_POSTGRES)?;
                assert_eq!(client.query("SELECT 1")?[0].get(0), Some("1"));
                Ok(())
            })
        })
    }
}
