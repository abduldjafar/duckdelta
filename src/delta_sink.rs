use std::collections::HashMap;

use aws_config::BehaviorVersion;
use deltalake::aws::constants::{
    AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_FORCE_CREDENTIAL_LOAD, AWS_REGION, AWS_S3_ALLOW_UNSAFE_RENAME
};
use deltalake::datafusion::prelude::{CsvReadOptions, SessionContext};
use deltalake::{
    open_table, open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError,
};

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

async fn crate_delta_table_from_csv(file_name: &str, delta_path: &str) -> Result<DeltaTable, Error> {
    let file_name_splitted: Vec<&str> = file_name.trim().split("/").collect();

    if file_name_splitted.is_empty() {
        return Err(DeltaTableError::Generic(
            "Invalid URI, should contain a file name".to_string(),
        )
        .into());
    };

    let ctx = SessionContext::new();
    
    let csv_table = ctx.read_csv(file_name, CsvReadOptions::new()).await?;

    let is_local_storage = is_local_storage(delta_path).await?;
    println!("{}", delta_path);

    match is_local_storage {
        true => {
            let table = DeltaOps::try_from_uri(delta_path)
                .await?
                .write(csv_table.collect().await?)
                .await?;
            Ok(table)
        }
        false => {
            deltalake::aws::register_handlers(None);

            let table = DeltaOps::try_from_uri_with_storage_options(delta_path, aws_config().await)
                .await?
                .write(csv_table.collect().await?)
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

async fn open_delta_table(uri: &str) -> Result<DeltaTable, DeltaTableError> {
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

#[cfg(test)]
mod tests {
    use csv::Writer;
    use deltalake::datafusion::prelude::SessionContext;
    use std::{env, fs,sync::Arc};

    use super::*;

    /// Generates a CSV file with sample data.
    async fn generate_data(file_name_str: &str) -> Result<(), Error> {
        let mut writer = Writer::from_path(file_name_str)?;

        // Write header and rows to the CSV
        let rows = [
            ["Name", "Age", "City"],
            ["Alice", "30", "New York"],
            ["Bob", "25", "San Francisco"],
            ["Charlie", "35", "Chicago"],
        ];
        for row in &rows {
            writer.write_record(row)?;
        }

        writer.flush()?;
        Ok(())
    }

    /// Tests whether `is_local_storage` correctly identifies URI types.
    #[tokio::test]
    async fn test_is_local_storage() -> Result<(), Error> {
        assert_eq!(is_local_storage("s3://mybucket/myfile.csv").await?, false);
        assert_eq!(is_local_storage("file:///path/to/myfile.csv").await?, true);

        assert!(is_local_storage("ftp://mybucket/myfile.csv").await.is_err());
        assert!(is_local_storage("https://mybucket/myfile.csv")
            .await
            .is_err());
        assert!(is_local_storage("mybucket/myfile.csv").await.is_err());
        assert!(is_local_storage("").await.is_err());
        assert!(is_local_storage("myfile.csv").await.is_err());

        Ok(())
    }

    /// Tests the `strip_file_prefix` function.
    #[tokio::test]
    async fn test_strip_file_prefix() -> Result<(), Error> {
        assert_eq!(
            strip_file_prefix("file:///path/to/myfile.csv").await?,
            "/path/to/myfile.csv"
        );

        assert!(strip_file_prefix("s3://mybucket/myfile.csv").await.is_ok());
        assert!(strip_file_prefix("ftp://mybucket/myfile.csv")
            .await
            .is_err());
        assert!(strip_file_prefix("https://mybucket/myfile.csv")
            .await
            .is_err());
        assert!(strip_file_prefix("mybucket/myfile.csv").await.is_err());
        assert!(strip_file_prefix("").await.is_err());
        assert!(strip_file_prefix("myfile.csv").await.is_err());

        Ok(())
    }

    /// Tests opening a Delta table.
    #[tokio::test]
    async fn test_open_delta_table() -> Result<(), Error> {
        let file_name_str = "output";
        let folder_path = "test_open_delta_table";

        let file_name = format!("{}.csv", file_name_str);
        let current_dir = env::current_dir().expect("Failed to get current directory");
        let full_path = current_dir.join(folder_path);
        let uri = format!("file://{}/{}", full_path.display(), file_name_str);

        // Generate sample data and create Delta table
        generate_data(&file_name).await?;
        crate_delta_table_from_csv(&file_name, &uri).await?;

        

        open_delta_table(&uri).await?;

        assert!(open_delta_table(&uri).await.is_ok());
        fs::remove_dir_all(folder_path)?;

        Ok(())
    }

    /// Tests creating a Delta table from a CSV file.
    #[tokio::test]
    async fn test_crate_delta_table_from_csv_at_local() -> Result<(), Error> {
        let file_name_str = "output";
        let file_name = format!("{}.csv", file_name_str);
        let current_dir = env::current_dir().expect("Failed to get current directory");
        let folder_path = "test_crate_delta_table_from_csv";
        let full_path = current_dir.join(folder_path);
        let uri = format!("file://{}/{}", full_path.display(), file_name_str);

        // Generate sample data and create Delta table
        generate_data(&file_name).await?;
        
        crate_delta_table_from_csv(&file_name, &uri).await?;

        // Open Delta table and register it with a SessionContext
        let table = open_table(&uri).await?;
        let ctx = SessionContext::new();
        ctx.register_table(file_name_str, Arc::new(table))?;

        // Execute SQL query and validate output
        let output = ctx
            .sql(format!("SELECT * FROM {}", file_name_str).as_str())
            .await?;
        output.clone().show().await?;

        assert!(!output.clone().collect().await?.is_empty());
        fs::remove_dir_all(folder_path)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_crate_delta_table_from_csv_at_s3() -> Result<(), Error> {
        let file_name_str = "output";
        let file_name = format!("{}.csv", file_name_str);
        let folder_path = "test_crate_delta_table_from_csv";
        let uri = format!("s3://datalake/{}/{}", folder_path, file_name_str);

        // Generate sample data and create Delta table
        generate_data(&file_name).await?;
        
        crate_delta_table_from_csv(&file_name, &uri).await?;

        // Open Delta table and register it with a SessionContext
        let table = open_table(&uri).await?;
        let ctx = SessionContext::new();
        ctx.register_table(file_name_str, Arc::new(table))?;

        // Execute SQL query and validate output
        let output = ctx
            .sql(format!("SELECT * FROM {}", file_name_str).as_str())
            .await?;
        output.clone().show().await?;

        assert!(!output.clone().collect().await?.is_empty());
        fs::remove_dir_all(folder_path)?;

        Ok(())
    }
}
