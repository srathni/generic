The Spark-based rule engine is configured and driven entirely through metadata tables. These tables capture all the configuration necessary to define batches, rules, data sources, output targets, mappings, and derivation logic. This approach ensures complete flexibility, auditability, and UI-driven scalability in the future.

✅ Metadata Tables Overview


Table Name	Purpose
AE_BATCH	Defines each batch (triggered via Autosys), includes batch-level metadata
AE_BATCH_RULES_MAP	Maps rules to batches and assigns their execution order (rank)
AE_RULES	Stores individual rule metadata, including type (SELECT, MERGE, UPDATE, PYTHON)
AE_BATCH_SOURCE_MAP	Lists data sources used in the batch, ensures they're loaded once
AE_SOURCES	Master list of available data sources, referenced in source mapping
AE_RULES_DATA_MAP	Stores SELECT rule column mappings, broken down column-by-column
AE_DATA_DICTIONARY	Master list of known tables and columns used for dropdowns and validation
AE_DERIVATION_LOOKUP	Stores configurable condition key-value pairs for event derivation
