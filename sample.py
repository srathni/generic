3. High-Level Architecture
The Spark-based ETL framework consists of the following key components:

Batch Orchestration System (AutoSys)
AutoSys is responsible for triggering batch jobs based on predefined schedules. It ensures that batches are executed in an orderly fashion, adhering to the dependency chain of jobs.
Batch Execution Engine (Spark)
Apache Spark serves as the core engine for processing large volumes of data in parallel. The framework runs Spark jobs, orchestrating SQL transformations, updates, and custom Python scripts as defined in each batch's configuration.
DataFrames: Data is loaded from various sources (e.g., S3, HDFS) into Spark DataFrames and transformed based on the defined rules.
Iceberg Table Management
Apache Iceberg is used for versioned data storage. The framework integrates Iceberg tables for transactional updates, enabling efficient read and write operations with ACID compliance.
Metadata Store
The AE_BATCH, AE_BATCH_RULES_MAP, and AE_BATCH_SOURCE_MAP tables serve as the central metadata store for managing batch execution, rules, and source data mappings. These tables are essential for dynamically determining which data sources and rules to execute for a given batch.
Kafka Logging and Monitoring
Kafka captures runtime metrics and logs, including execution time, rule-level success/failure, and processed record counts. These logs are critical for tracking the health of batch jobs, generating operational metrics, and feeding monitoring dashboards.
Data Sources and Outputs
The framework supports reading from various data sources (e.g., Parquet, Iceberg) and writing the results to a specified destination (e.g., S3, HDFS, databases). Outputs may be in the form of transformed Parquet files, updated Iceberg tables, or other formats as configured in the rule definitions.
