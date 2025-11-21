from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys

# Inicializar contexto Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME']) if 'JOB_NAME' in sys.argv else {}
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
if 'JOB_NAME' in args:
    job.init(args['JOB_NAME'], args)


# 1. Credenciales y Endpoint de la BD RDS

DB_ENDPOINT = "db-covid-data.cyrm8g8cqcwk.us-east-1.rds.amazonaws.com" 
DB_USER = "admin"
DB_PASSWORD = "database#1397Ltlt."
DB_NAME = "covid_data_db"
DB_TABLE = "datos_municipios_complementarios"
DB_URL = f"jdbc:mysql://{DB_ENDPOINT}:3306/{DB_NAME}"
DB_DRIVER = "com.mysql.cj.jdbc.Driver" # Driver para MySQL

# 2. Configuración S3 (Zona RAW)
S3_BUCKET = "buket-covid-colombia-proyecto3"
RAW_PREFIX = "raw/rds/municipios/"
# ==============================================================================

def main():
    print(f"Iniciando ingesta directa desde RDS (Tabla: {DB_TABLE})")
    
    # 1. Leer datos desde RDS usando la conexión JDBC programática
    # Spark leerá directamente usando los parámetros en el código.
    try:
        df_rds = spark.read.format("jdbc").options(
            url=DB_URL,
            dbtable=DB_TABLE,
            user=DB_USER,
            password=DB_PASSWORD,
            driver=DB_DRIVER
        ).load()
    except Exception as e:
        # Esto capturará fallos de conexión a RDS (puerto 3306)
        print(f"ERROR: Fallo al leer la base de datos. Esto puede ser un error de Security Group (3306) o credenciales.")
        raise e
    
    print(f"Total de registros leídos: {df_rds.count()}")

    # 2. Definir la ruta de salida en S3/Raw con partición de ingesta
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    raw_s3_path = f"s3://{S3_BUCKET}/{RAW_PREFIX}date_ingesta={timestamp}/"

    print(f"Escribiendo en S3 RAW: {raw_s3_path}")
    
    # 3. Escribir los datos en formato Parquet
    df_rds.write.mode("overwrite").parquet(raw_s3_path)
    
    print("Ingesta de RDS a RAW finalizada correctamente.")

if __name__ == "__main__":
    main()
    if 'JOB_NAME' in args:
        job.commit()