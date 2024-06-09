CREATE EXTERNAL TABLE IF NOT EXISTS `roisin-database`.`accelerometer_trusted` (
  `z` float,
  `user` string,
  `y` float,
  `x` float,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://roisin-bucket/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');