use std::collections::HashMap;

use deltalake::aws::constants::{AWS_FORCE_CREDENTIAL_LOAD, AWS_S3_ALLOW_UNSAFE_RENAME};
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{col, SessionContext};
use deltalake::{open_table, open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError};
use duckdb::{Connection, Error as DuckDBError};
use duckdb::arrow::record_batch::RecordBatch;

#[derive(Clone, Debug)]
pub enum Error {
    DataFusion(String),
    DuckDB(String),
    Delta(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Error::DataFusion(error.to_string())
    }
}

impl From<DuckDBError> for Error {
    fn from(error: DuckDBError) -> Self {
        Error::DuckDB(error.to_string())
    }
}

impl From<deltalake::DeltaTableError> for Error {
    fn from(error: deltalake::DeltaTableError) -> Self {
        Error::Delta(error.to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r#"
        INSTALL parquet;
        LOAD parquet;
        INSTALL delta;
        LOAD delta;
        "#,
    )?;

    let delta_path = "/delta_source/public_payment";
    let delta_sink = "/delta_sink/public_payment";
    
    DeltaOps(open_table(delta_sink).await?)
        .merge(
            SessionContext::new().read_batches(
                conn.prepare(&format!(
                    r#"
                    SELECT * 
                    FROM delta_scan('{delta_path}')
                    "#
                ))?
                .query_arrow([])?
                .collect::<Vec<RecordBatch>>(),
            )?,
            col("target.payment_id").eq(col("source.payment_id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            let columns = vec!["amount","customer_id","amount"];
            columns.iter().fold(update, |update, &column| {
                update.update(column, col(&format!("source.{}", column)))
            })
        })?
        .when_not_matched_insert(|insert| {
            let columns = vec!["amount","customer_id","amount"];
            columns.iter().fold(insert, |insert, &column| {
                insert.set(column, col(&format!("source.{}", column)))
            })
        })?
        .await?;

    Ok(())
}


async fn is_local_storage(uri: &str) -> Result<bool, Error> {
    if uri.starts_with("s3://") {
        Ok(false)
    } else if uri.starts_with("file://") {
        Ok(true)
    } else {
        Err(DeltaTableError::Generic(
            "storage type should start with either s3:// or file://".to_string(),
        ).into())
    }
}

/// strips file:/// prefix from the uri
async fn strip_file_prefix(uri: &str) -> Result<&str, Error> {
    if let Some(path) = uri.strip_prefix("file://") {
        Ok(path)
    } else {
        panic!("strip_file_prefix called on a non-file uri")
    }
}

async fn open_delta_table(uri: &str) -> Result<DeltaTable, Error> {
    let is_local_storage = is_local_storage(uri).await?;
    if is_local_storage {
        Ok(open_table(uri).await?)
    } else {
        let uri = strip_file_prefix(uri).await?;
        Ok(open_table_with_storage_options(uri, aws_config()).await?)
    }
}

fn aws_config() -> HashMap<String, String> {
    let mut storage_options = HashMap::new();

    storage_options.insert(AWS_FORCE_CREDENTIAL_LOAD.to_string(), "true".to_string());
    storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());

    storage_options
}