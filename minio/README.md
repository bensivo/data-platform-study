# datalake

A basic datalake built on object storage. For this project, I'm using a local instance of MinIO.


This datalake uses Databricks' medallion architecture, with 4 folders:
- raw - Raw records, as they were produced by applications
- bronze - Raw records, loaded into tabular format
- silver - Transformed records, with data quality checks, standardized naming conventions, and well-defined schemas
- gold - Aggregated records, ready for consumption by BI tools

