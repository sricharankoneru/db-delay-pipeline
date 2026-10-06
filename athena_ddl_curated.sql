CREATE EXTERNAL TABLE `db_delay_pipeline.curated`(
  `pulled_at` string, 
  `trip_id` bigint, 
  `stop_id` bigint, 
  `station_name` string, 
  `delay_sec` bigint)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://db-delay-pipeline-sricharan-274535057566-eu-central-1-an/curated'
TBLPROPERTIES (
  'transient_lastDdlTime'='1790105310')