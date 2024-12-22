use crate::error::Error;
use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;

mod delta_sink;

#[async_trait]
pub trait Sinks {
    async fn write(&self, data: &Vec<RecordBatch>, folder_path: &str) -> Result<(), Error>;
    async fn merge_update(
        &self,
        table_path: &str,
        data_batches: &Vec<RecordBatch>,
        key_column: &str,
        target_column: &[&str],
    ) -> Result<(), Error>;
}

pub struct Delta {
    path: String,
}

impl Delta {
    pub fn new(path: &str) -> Self {
        Delta {
            path: path.to_string(),
        }
    }
}

#[async_trait]
impl Sinks for Delta {
    async fn write(&self, data: &Vec<RecordBatch>, folder_path: &str) -> Result<(), Error> {
        let full_path = format!("{}/{}", self.path, folder_path);
        delta_sink::write(&full_path, data).await?;
        Ok(())
    }

    async fn merge_update(
        &self,
        table_path: &str,
        data_batches: &Vec<RecordBatch>,
        key_column: &str,
        target_column: &[&str],
    ) -> Result<(), Error> {
        delta_sink::merge_update(table_path, data_batches, key_column, target_column).await?;
        Ok(())
    }
}
