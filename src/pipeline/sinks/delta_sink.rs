use std::collections::HashMap;

use aws_config::BehaviorVersion;
use deltalake::aws::constants::{
    AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_FORCE_CREDENTIAL_LOAD, AWS_S3_ALLOW_UNSAFE_RENAME,
};
use deltalake::{
    open_table, open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError,
};

use deltalake::arrow::array::RecordBatch;

use crate::error::Error;

async fn is_local_storage(uri: &str) -> Result<bool, DeltaTableError> {
    if uri.starts_with("s3://") {
        Ok(false)
    } else if uri.starts_with("file://") {
        Ok(true)
    } else {
        Err(DeltaTableError::Generic(
            "storage type should start with either s3:// or file://".to_string(),
        )
        .into())
    }
}

pub(crate) async fn write(delta_path: &str, data: Vec<RecordBatch>) -> Result<DeltaTable, Error> {
    // Check if the delta path is local or on AWS
    let is_local_storage = is_local_storage(delta_path).await?;

    match is_local_storage {
        true => {
            // Write to local storage
            let table = DeltaOps::try_from_uri(delta_path)
                .await?
                .write(data)
                .await?;
            Ok(table)
        }
        false => {
            // Register AWS handlers and write to AWS storage
            deltalake::aws::register_handlers(None);
            let table = DeltaOps::try_from_uri_with_storage_options(delta_path, aws_config().await)
                .await?
                .write(data)
                .await?;
            Ok(table)
        }
    }
}

/// strips file:/// prefix from the uri
async fn strip_file_prefix(uri: &str) -> Result<&str, DeltaTableError> {
    if let Some(path) = uri.strip_prefix("file://") {
        Ok(path)
    } else if let Some(path) = uri.strip_prefix("s3://") {
        let _ = path;
        Ok(uri)
    } else {
        return Err(DeltaTableError::Generic(
            "strip_file_prefix called on a non-file uri".to_string(),
        ));
    }
}

async fn _open_delta_table(uri: &str) -> Result<DeltaTable, DeltaTableError> {
    let is_local_storage = is_local_storage(uri).await?;
    if is_local_storage {
        open_table(uri).await
    } else {
        deltalake::aws::register_handlers(None);
        let uri = strip_file_prefix(uri).await?;
        open_table_with_storage_options(uri, aws_config().await).await
    }
}

async fn aws_config() -> HashMap<String, String> {
    let mut storage_options = HashMap::new();

    storage_options.insert(AWS_FORCE_CREDENTIAL_LOAD.to_string(), "true".to_string());
    storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());
    let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;

    match config.endpoint_url() {
        Some(endpoint) => {
            storage_options.insert(AWS_ENDPOINT_URL.to_string(), endpoint.to_string());
            storage_options.insert(AWS_ALLOW_HTTP.to_string(), "true".to_string());
        }
        None => (),
    }

    storage_options
}
