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
