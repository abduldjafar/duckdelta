use async_trait::async_trait;

use deltalake::{
    arrow::array::RecordBatch,
    datafusion::prelude::{CsvReadOptions, SessionContext},
};

use crate::error::Error;

pub enum SourcesType<'a> {
    Csv(&'a str),
    Json(&'a str),
    Parquet(&'a str),
    Delta(&'a str),
}

#[async_trait]
pub trait Sources {
    async fn read_data(&self) -> Result<Vec<RecordBatch>, Error>;
}

impl<'a> SourcesType<'a> {
    async fn read(&self) -> Result<Vec<RecordBatch>, Error> {
        match self {
            SourcesType::Csv(path) => {
                let path_splitted: Vec<&str> = path.split(".").collect();
                let record_batches = match path_splitted.last().unwrap().to_lowercase().as_str() {
                    "csv" => {
                        let ctx = SessionContext::new();
                        let df = ctx.read_csv(*path, CsvReadOptions::new()).await?;
                        df.collect().await?
                    }
                    _ => return Err(Error::UnsupportedFormat(path.to_string())),
                };
                Ok(record_batches)
            }
            SourcesType::Json(_) => unimplemented!(),
            SourcesType::Parquet(_) => unimplemented!(),
            SourcesType::Delta(_) => unimplemented!(),
        }
    }
}

#[async_trait]
impl<'a> Sources for SourcesType<'a> {
    async fn read_data(&self) -> Result<Vec<RecordBatch>, Error> {
        self.read().await
    }
}
