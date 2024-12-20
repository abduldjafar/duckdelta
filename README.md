# DuckDelta

**DuckDelta** is a streamlined data pipeline solution designed to connect multiple data sources with [Delta Lake](https://delta.io/), leveraging the high-performance query capabilities of [DuckDB](https://duckdb.org/). With **DuckDelta**, you can efficiently process and transform data using SQL before loading it into Delta Lake, combining lightweight in-memory analytics with robust distributed storage and transactional guarantees.

This tool is perfect for users who prefer SQL for data processing and seek a simple way to ingest their results into Delta Lake. While DuckDB currently supports reading Delta Lake data, **DuckDelta** bridges the gap by enabling seamless writing to Delta Lake as part of the pipeline.

Optimized for on-premise environments, DuckDelta also includes built-in support for [MinIO](https://min.io/) as a storage backend, ensuring compatibility with S3-like storage solutions.

---

## Key Features

- **SQL-Based Data Processing**:
  - Use DuckDB's SQL engine for in-memory transformations and analytics.
  - Easily move SQL query results into Delta Lake.

- **Data Pipeline Creation**:
  - Connect multiple data sources (CSV, Parquet, JSON, etc.) and load them into Delta Lake.
  - Simplify ETL/ELT workflows with SQL-first design.

- **Delta Lake Integration**:
  - Write processed data directly to Delta Lake for long-term storage.
  - Benefit from ACID transactions, schema evolution, and distributed storage support.

- **Source and Storage Compatibility**:
  - Supports popular file formats like CSV, Parquet, and JSON.
  - Compatible with MinIO, AWS S3, Google Cloud Storage, Azure Blob Storage, and more.

---

## Use Cases

- **SQL-Driven ETL Pipelines**:
  Simplify ETL pipelines for SQL users who want to process data and load it into Delta Lake without complex setups.

- **Data Transformation**:
  Perform SQL-based transformations in DuckDB and write the results to Delta Lake.

- **Hybrid Storage and Analytics**:
  Combine DuckDB's in-memory speed with Delta Lake's scalable and durable storage.

- **On-Premise Solutions**:
  Use MinIO or other S3-compatible storage solutions for on-premise deployments.

---

## Roadmap

- [ ] Implement connectors for popular data sources (e.g., PostgreSQL, MySQL, APIs).
- [ ] Support advanced Delta Lake operations (`MERGE`, `UPDATE`, `DELETE`).
- [ ] Add support for Delta Lake writes directly via DuckDelta.
- [ ] Provide templates for SQL-driven ETL workflows.
- [ ] Optimize for distributed storage backends like MinIO and AWS S3.
- [ ] Add monitoring tools for pipeline execution and performance metrics.

---

## Contributing

We welcome contributions to **DuckDelta**! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

---

## License

DuckDelta is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Acknowledgements

DuckDelta builds on the powerful capabilities of DuckDB and Delta Lake. We extend our gratitude to the open-source community for their contributions to these technologies.

---