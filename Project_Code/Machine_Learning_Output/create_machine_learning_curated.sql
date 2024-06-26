CREATE EXTERNAL TABLE IF NOT EXISTS `roisin-database`.`machine_learning_curated` (
  `serialNumber` string,
  `z` float,
  `y` float,
  `sensorReadingTime` bigint,
  `x` float,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://roisin-bucket/spark_output/'
TBLPROPERTIES ('classification' = 'json');