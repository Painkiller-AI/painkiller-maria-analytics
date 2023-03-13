CREATE EXTERNAL TABLE IF NOT EXISTS `maria_analytics_dev`.`messages` (
  `id` string,
  `created_at` string,
  `data` struct<`author`:struct<`id`:string,`type`:string>,
                `conversation_id`: string
                >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nabla-analytics-dev/conversation/message/created/'
TBLPROPERTIES ('classification' = 'json');
