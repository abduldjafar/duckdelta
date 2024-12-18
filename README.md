# DuckDelta

**DuckDelta** is a hybrid data solution combining the blazing-fast analytical query engine of [DuckDB](https://duckdb.org/) with the transactional capabilities and distributed storage support of [Delta Lake](https://delta.io/). DuckDelta brings the best of both worlds: efficient in-memory analytics with DuckDB and the flexibility of Delta Lake for managing large-scale, distributed datasets with features like ACID transactions, schema evolution, and `MERGE`/`UPDATE` operations.

DuckDelta is designed for on-premise environments, enabling organizations to leverage existing infrastructure. With support for [MinIO](https://min.io/) as a storage backend, DuckDelta ensures high performance and compatibility with S3-like storage solutions.

---

## Key Features

- **DuckDB Analytics Engine**:
  - Lightweight and fast analytical queries for structured data.
  - Support for SQL queries with advanced functions and optimizations.
  - In-memory processing for rapid insights.

- **Delta Lake Capabilities**:
  - ACID transactions for consistent data updates.
  - Schema evolution and enforcement for structured datasets.
  - Support for `MERGE`, `UPDATE`, and `DELETE` operations.
  - Distributed storage compatibility (e.g., MinIO, AWS S3, Google Cloud Storage, Azure).

- **Unified Interface**:
  - Leverages DuckDB for compute and Delta Lake for storage and transactional capabilities.
  - Query and modify Delta Lake data directly with DuckDB.

---

## Use Cases

- **Ad-hoc Analytics**:
  Use DuckDB to quickly query Delta Lake data without complex cluster setups.

- **Data Lakes**:
  Manage distributed datasets in Delta Lake format while benefiting from DuckDB's speed.

- **Transactional Workflows**:
  Enable `MERGE`/`UPDATE` operations on large datasets stored in Delta Lake.

- **Hybrid Storage**:
  Use MinIO or other S3-compatible storage solutions for Delta Lake datasets while performing local analytics with DuckDB.

---

## Roadmap

- [ ] Implement core integration between DuckDB and Delta Lake.
- [ ] Support distributed storage formats like Parquet and ORC.
- [ ] Add support for advanced Delta Lake features (`MERGE`, `UPDATE`, `DELETE`).
- [ ] Benchmark performance for large-scale datasets.
- [ ] Provide support for async queries and operations.
- [ ] Optimize for on-premise deployments with MinIO.

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description.

---

## License

DuckDelta is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Acknowledgements

DuckDelta is inspired by the powerful capabilities of DuckDB and Delta Lake. Special thanks to the open-source community for their contributions to these technologies.
