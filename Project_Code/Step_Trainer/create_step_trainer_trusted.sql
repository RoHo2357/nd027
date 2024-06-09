CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://roisin-bucket/step_trainer/trusted'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1717950414')