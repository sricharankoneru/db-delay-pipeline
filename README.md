# Deutsche Bahn Real-Time Delay Pipeline

An AWS data engineering pipeline that ingests near-real-time delay data
for Deutsche Bahn trains at Germany's 16 state-capital stations, stores
and transforms it in a data lake, and visualizes punctuality trends in
a Tableau dashboard.

## Architecture
AWS Lambda (ingestion) → S3 (raw) → Step Functions (orchestration) →
Glue Crawler + ETL (transform) → S3 (curated, Parquet) → Athena → Tableau

## Status
Work in progress. Full documentation, architecture diagram, and
dashboard link coming soon.
