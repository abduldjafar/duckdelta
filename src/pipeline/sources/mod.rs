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

impl <'a> SourcesType <'a> {
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
    async fn read_data(&self) -> Result<Vec<RecordBatch>, Error>{
        self.read().await
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use csv::Writer;

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
    async fn test_csv_sources()-> Result<(), Error> {

        let folder_test = "test_csv_sources";
        let file1 = format!("{}/file1.csv", folder_test);

        fs::create_dir(folder_test)?;
        generate_data(&file1).await?;

        let csv_sources = SourcesType::Csv(&file1);
        let record_batches = csv_sources.read_data().await?;


        assert!(!record_batches.is_empty());

        fs::remove_dir_all(folder_test)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_csv_sources_with_asterix()-> Result<(), Error> {

        let folder_test = "test_csv_sources_with_asterix";
        let file1 = format!("{}/file1.csv", folder_test);
        let file2 = format!("{}/file2.csv", folder_test);
        let path_for_test = format!("{}/*.csv", folder_test);

        fs::create_dir(folder_test)?;
        generate_data(&file1).await?;
        generate_data(&file2).await?;

        let csv_sources = SourcesType::Csv(&path_for_test);
        let record_batches = csv_sources.read_data().await?;


        assert!(!record_batches.is_empty());

        fs::remove_dir_all(folder_test)?;
        Ok(())
    }


}
