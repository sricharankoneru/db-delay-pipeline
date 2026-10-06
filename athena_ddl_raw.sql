CREATE EXTERNAL TABLE `db_delay_pipeline.raw`(
  `pulled_at` string, 
  `trip_id` bigint, 
  `stop_id` bigint, 
  `station_name` string, 
  `delay_sec` bigint)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://db-delay-pipeline-sricharan-274535057566-eu-central-1-an/raw/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='cfbd4252-0c8a-4b6c-bb0c-b83e98393b8e', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'DEPRECATED_BY_CRAWLER'='1790211687660', 
  'UPDATED_BY_CRAWLER'='db_delay_raw_crawler', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='52', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='957', 
  'partition_filtering.enabled'='true', 
  'recordCount'='1206', 
  'sizeKey'='71025', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')