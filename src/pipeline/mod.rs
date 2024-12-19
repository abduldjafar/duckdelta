use deltalake::arrow::array::RecordBatch;
use engines::Engine;
use sinks::Sinks;
use sources::{Sources, SourcesType};

use crate::error::Error;

pub mod engines;
pub mod sinks;
pub mod sources;

pub struct Pipeline<Exc: Engine> {
    enginee: Exc,
    record_batches: Option<Vec<RecordBatch>>,
}

impl<Exc: Engine> Pipeline<Exc> {
    pub async fn new(enginee: Exc) -> Result<Pipeline<Exc>, Error> {
        Ok(Pipeline {
            enginee: enginee,
            record_batches: None,
        })
    }

    pub async fn read_csv(&mut self, path: &str) -> Result<&mut Self, Error> {
        let path_splitted: Vec<&str> = path.split(".").collect();
        let record_batches = match path_splitted.last().unwrap().to_lowercase().as_str() {
            "csv" => {
                let data = SourcesType::Csv(path);

                data.read_data().await?
            }
            _ => return Err(Error::UnsupportedFormat(path.to_string())),
        };
        self.record_batches = Some(record_batches);
        Ok(self)
    }

    pub async fn write_delta(&mut self, bucket_name: &str, tb_name: &str) -> Result<(), Error> {
        let sink = sinks::Delta::new(bucket_name);
        self.enginee
            .delta_table_mapping(bucket_name, tb_name)
            .await?;
        sink.write(self.record_batches.as_ref().unwrap().to_vec(), tb_name)
            .await?;
        Ok(())
    }

    pub async fn execute_sql(&mut self, query: &str) -> Result<&mut Self, Error> {
        // Execute the SQL query using the engine and update the record_batches
        let result = self.enginee.sql(query).await?;
        self.record_batches = Some(result);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use csv::Writer;
    use deltalake::datafusion::{
        config::SqlParserOptions,
        prelude::{ParquetReadOptions, SessionContext},
        sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser},
    };
    use engines::DuckDB;

    use super::*;

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

    #[tokio::test]
    async fn test_pipeline() -> Result<(), Error> {
        let folder_test = "/Users/abdulharisdjafar/Documents/private/code/duckdelta/test_pipeline";
        let file1 = format!("{}/file1.csv", folder_test);
        let delta_place = format!("file://{}", folder_test);

        //fs::remove_dir_all(folder_test)?;
        fs::create_dir(folder_test)?;
        generate_data(&file1).await?;
        let duck_engine = DuckDB::new().await?;

        let mut pipeline = Pipeline::new(duck_engine).await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&delta_place, "tb_1")
            .await?;

        pipeline
            .execute_sql("SELECT 1")
            .await?
            .write_delta(&delta_place, "tb_2")
            .await?;

        // Verify the contents of delta1 and delta2

        let ctx = SessionContext::new();
        let df1 = ctx
            .read_parquet(
                format!("{}/tb_1/*.parquet", folder_test),
                ParquetReadOptions::new(),
            )
            .await;
        let df2 = ctx
            .read_parquet(
                format!("{}/tb_2/*.parquet", folder_test),
                ParquetReadOptions::new(),
            )
            .await;

        assert!(df1.is_ok());
        assert!(df2.is_ok());

        fs::remove_dir_all(folder_test)?;

        Ok(())
    }
}
