use crate::error::Error;
use async_trait::async_trait;
use deltalake::{
    arrow::array::RecordBatch,
    datafusion::prelude::{CsvReadOptions, SessionContext},
};

mod delta_sink;

#[async_trait]
pub trait Sinks {
    async fn write(&self, data: Vec<RecordBatch>, folder_path: &str) -> Result<(), Error>;
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
    async fn write(&self, data: Vec<RecordBatch>, folder_path: &str) -> Result<(), Error> {
        let full_path = format!("{}/{}", self.path, folder_path);
        delta_sink::write(&full_path, data).await?;
        Ok(())
    }
}
