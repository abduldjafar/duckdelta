use std::{fs, io, path::Path, time::Instant};

use csv::Writer;
use deltalake::datafusion::prelude::{ParquetReadOptions, SessionContext};
use duckdelta::{
    error::Error,
    pipeline::{engines, Pipeline},
};
use engines::DuckDB;
use tokio::task;

/// Get a list of all CSV files in a folder and its subfolders with full paths
fn get_csv_files(folder: &str) -> io::Result<Vec<String>> {
    let mut csv_files = Vec::new();
    let folder_path = Path::new(folder);

    // Ensure the provided folder exists
    if !folder_path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Provided path is not a directory",
        ));
    }

    // Traverse the directory recursively
    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Recurse into subdirectories
            csv_files.extend(get_csv_files(path.to_str().unwrap())?);
        } else if let Some(extension) = path.extension() {
            if extension == "csv" {
                // Add CSV files to the list
                csv_files.push(path.to_str().unwrap().to_string());
            }
        }
    }

    Ok(csv_files)
}

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

    fs::remove_dir_all(folder_test)?;

    Ok(())
}

#[tokio::test]
async fn test_parallel_pipeline() -> Result<(), Error> {
    let list_files = get_csv_files("src/raw_data")?;

    let mut tasks = Vec::new();

    let test_start_time = Instant::now();

    for file in list_files {
        let file_clone = file.clone();
        tasks.push(task::spawn(async move {
            let task_start_time = Instant::now();

            let table_path_arr: Vec<&str> =
                file_clone.split("/").last().unwrap().split(".").collect();
            let table_path = table_path_arr.first().unwrap();
            println!("Start processing: {}", table_path);

            let duck_engine = DuckDB::new().await.unwrap();
            let mut pipeline = Pipeline::new(duck_engine).await.unwrap();
            pipeline
                .read_csv(&file_clone)
                .await
                .unwrap()
                .write_delta("s3://datalake", table_path)
                .await
                .unwrap();

            let elapsed_time = task_start_time.elapsed();
            println!(
                "Pipeline execution time for {}: {:?}",
                table_path, elapsed_time
            );
        }));
    }

    for task in tasks {
        task.await.unwrap(); // Ensure no panic during task execution
    }

    let test_elapsed_time = test_start_time.elapsed();
    println!("Total test execution time: {:?}", test_elapsed_time);

    Ok(())
}
