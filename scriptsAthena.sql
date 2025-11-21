CREATE DATABASE IF NOT EXISTS covid_colombia_analisis;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_colombia_analisis.resumen_departamental (

`nombre_departamento` string,

`total_casos_por_depto` bigint,

`edad_promedio_depto` double,

`dias_promedio_recuperacion` double

)ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

LOCATION 's3://buket-covid-colombia-proyecto3/refined/resumen_analitico/resumen_departamental/';


CREATE EXTERNAL TABLE IF NOT EXISTS covid_colombia_analisis.resumen_sexo_estado (

`sexo` string,

`estado` string,

`total_casos` bigint

)ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

LOCATION 's3://buket-covid-colombia-proyecto3/refined/resumen_analitico/sexo_estado/';
