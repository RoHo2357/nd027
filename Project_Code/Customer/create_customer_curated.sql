CREATE EXTERNAL TABLE IF NOT EXISTS `roisin-database`.`customer_curated` (
  `serialNumber` string,
  `birthDay` date,
  `shareWithResearchAsOfDate` bigint,
  `registrationDate` bigint,
  `customerName` string,
  `shareWithFriendsAsOfDate` bigint,
  `email` string,
  `lastUpdateDate` bigint,
  `phone` string,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://roisin-bucket/customer/curated/'
TBLPROPERTIES ('classification' = 'json');