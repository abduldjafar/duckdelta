use deltalake::{arrow::array::RecordBatch, datafusion::prelude::SessionContext};
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
        if path.ends_with(".csv") {
            let data = SourcesType::Csv(path);
            self.record_batches = Some(data.read_data().await?);
            Ok(self)
        } else {
            Err(Error::UnsupportedFormat(path.to_string()))
        }
    }

    pub async fn write_delta(&mut self, bucket_name: &str, tb_name: &str) -> Result<(), Error> {
        let sink = sinks::Delta::new(bucket_name);
        self.enginee.delta_table_mapping(
            &format!("{}/{}", bucket_name, tb_name),
            &format!("delta_{}", tb_name),
        )?;

        let data_batches = match self.record_batches.as_ref() {
            Some(batches) => batches,
            None => {
                return Err(Error::Delta(
                    "Record batches are not initialized".to_string(),
                ))
            }
        };

        sink.write(data_batches, tb_name).await?;
        Ok(())
    }

    pub async fn merge_update(
        &mut self,
        bucket_name: &str,
        table_path: &str,
        key_column: &str,
        target_column: &[&str],
    ) -> Result<(), Error> {
        let sink = sinks::Delta::new(bucket_name);
        let full_path = format!("{}/{}", bucket_name, table_path);

        let data_batches = match self.record_batches.as_ref() {
            Some(batches) => batches,
            None => {
                return Err(Error::Delta(
                    "Record batches are not initialized".to_string(),
                ))
            }
        };

        sink.merge_update(&full_path, data_batches, key_column, target_column)
            .await?;
        Ok(())
    }

    pub async fn execute_sql(&mut self, query: &str) -> Result<&mut Self, Error> {
        let result = self.enginee.sql(query)?;
        self.record_batches = Some(result);
        Ok(self)
    }

    pub async fn show(&mut self) -> Result<(), Error> {
        let session = SessionContext::new();
        let df = session.read_batches(self.record_batches.clone().unwrap())?;
        df.show().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use csv::Writer;
    use deltalake::datafusion::prelude::{ParquetReadOptions, SessionContext};
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

    async fn generate_second_data(file_name_str: &str) -> Result<(), Error> {
        let mut writer = Writer::from_path(file_name_str)?;

        // Write header and rows to the CSV
        let rows = [
            ["Name", "Age", "City"],
            ["Alice", "12", "New York"],
            ["Bob", "11", "San Francisco"],
            ["Charlie", "45", "Chicago"],
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
        let local_delta_place = format!("file://{}", folder_test);
        let s3_delta_place = format!("s3://datalake");

        // test in local
        fs::create_dir(folder_test)?;
        generate_data(&file1).await?;
        let duck_engine = DuckDB::new().await?;

        let mut pipeline = Pipeline::new(duck_engine).await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&s3_delta_place, "tb_delta")
            .await?;

        pipeline
            .execute_sql("SELECT * from delta_tb_delta")
            .await?
            .write_delta(&s3_delta_place, "tb_from_delta")
            .await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&local_delta_place, "tb_1")
            .await?;

        pipeline
            .execute_sql("SELECT * from delta_tb_1")
            .await?
            .write_delta(&local_delta_place, "tb_2")
            .await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&s3_delta_place, "tb_1")
            .await?;

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

    #[tokio::test]
    async fn test_merge_update_pipeline() -> Result<(), Error> {
        let folder_test =
            "/Users/abdulharisdjafar/Documents/private/code/duckdelta/test_merge_update_pipeline";
        let file1 = format!("{}/file1.csv", folder_test);
        let file2 = format!("{}/file2.csv", folder_test);
        let s3_delta_place = format!("s3://datalake");

        // test in local
        fs::create_dir(folder_test)?;
        generate_data(&file1).await?;
        generate_second_data(&file2).await?;
        let duck_engine = DuckDB::new().await?;

        let mut pipeline = Pipeline::new(duck_engine).await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&s3_delta_place, "tb_delta_merge")
            .await?;

        pipeline
            .read_csv(&file1)
            .await?
            .write_delta(&s3_delta_place, "tb_delta_merge_2")
            .await?;

        pipeline
            .read_csv(&file2)
            .await?
            .merge_update(&s3_delta_place, "tb_delta_merge", "Name", &["Age", "City"])
            .await?;


        pipeline
            .execute_sql(r#"
                select
                    *
                from
                delta_tb_delta_merge_2
            "#)
            .await?
            .merge_update(&s3_delta_place, "tb_delta_merge", "Name", &["Age", "City"])
            .await?;

        fs::remove_dir_all(folder_test)?;

        Ok(())
    }
}
