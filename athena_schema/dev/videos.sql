CREATE EXTERNAL TABLE IF NOT EXISTS `maria_analytics_prd`.`videos` (
  `id` string,
  `start_at` string,
  `patient` string,
  `provider` string,
  `finish_at` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nabla-analytics-dev/videos_analytics/'
TBLPROPERTIES ('classification' = 'json');
