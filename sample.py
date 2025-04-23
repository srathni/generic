The new Spark-based ETL framework is designed around the concept of batches and rules. Each batch represents a logical unit of work, triggered externally via an AutoSys job scheduler. Batches are composed of one or more rules, which define the specific transformations or operations to be executed. These rules can include standard SQL operations such as SELECT, UPDATE, and MERGE (typically performed on Apache Iceberg tables), or custom logic written in Python, referred to as "scripts".

Each rule within a batch is assigned a rank to determine its execution order. The metadata for batch execution is maintained in relational tables:

AE_BATCH: Stores metadata about each batch, including identifiers and scheduling details.
AE_BATCH_RULES_MAP: Defines the set of rules associated with each batch, including their type (SQL/script), execution order, and output targets.
AE_BATCH_SOURCE_MAP: Specifies the input datasets for the batch. At runtime, the framework reads these source definitions, materializes them as Spark DataFrames, and makes them available in the Spark session for use by any script or SQL rule during execution.
As each batch runs, the framework reads the AE_BATCH_RULES_MAP, executes each rule in the defined order, and writes the output as configured. Logging is centralized via Kafka, capturing metrics such as rule-level runtime, record counts, and error information, which are later used for monitoring, auditing, and dashboarding.

This design promotes modularity, traceability, and extensibility, allowing the ETL framework to scale with growing data and evolving logic requirements.
