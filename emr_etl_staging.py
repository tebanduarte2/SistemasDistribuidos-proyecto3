from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date

# Inicializar Spark Session
spark = SparkSession.builder.appName("ETL_Covid_Staging_Final_V15_Fechas").getOrCreate()


# CONFIGURACION DE RUTAS
RAW_COVID_PATH = "s3://buket-covid-colombia-proyecto3/raw/covid/"
RAW_MUNICIPIOS_PATH = "s3://buket-covid-colombia-proyecto3/raw/rds/municipios/"
# La salida debe ser en Trusted
TRUSTED_PATH = "s3://buket-covid-colombia-proyecto3/trusted/covid_unificado/" 


# Nombres limpios (24 columnas - incluyendo la extra 'date')
nombres_originales_limpios = [
    "fecha_reporte_web", "id_caso", "fecha_notificacion", "cod_divipola_depto", 
    "nombre_departamento", "codigo_dane_municipio", "nombre_municipio", "edad", 
    "unidad_medida_edad", "sexo", "tipo_contagio", "ubicacion_caso", "estado", 
    "cod_iso_pais", "nombre_pais", "recuperado", "fecha_inicio_sintomas", 
    "fecha_muerte", "fecha_diagnostico", "fecha_recuperacion", 
    "tipo_recuperacion", "pertenencia_etnica", "nombre_grupo_etnico",
    "columna_extra_date" 
]

try:
    # 1. LECTURA DE DATOS RAW
    print("Paso 1: Leyendo datos de COVID y renombrando (24 columnas)...")
    df_raw = spark.read.csv(
        RAW_COVID_PATH, 
        header=True,       
        inferSchema=False, 
        sep=','
    ).toDF(*nombres_originales_limpios)

    # 2. SELECCIÓN, CASTING Y LIMPIEZA
    print("Paso 2: Aplicando tipos de dato y seleccionando columnas clave...")
    
    # *** CORRECCIÓN CLAVE: SELECCIONAR MÁS COLUMNAS DE FECHA ***
    df_covid = df_raw.select(
        col("codigo_dane_municipio").cast("string"),
        to_date(col("fecha_notificacion")).alias("fecha_notificacion"),
        trim(lower(col("nombre_departamento"))).alias("nombre_departamento"),
        col("edad").cast("int"),
        col("recuperado"),
        col("estado"),
        col("sexo"),
        col("tipo_contagio"),
        col("fecha_diagnostico"), 
        col("fecha_recuperacion"),
        col("fecha_muerte")       
    )


    # 3. MUNICIPIOS Y JOIN
    print("Paso 3: Cruzando con Municipios...")
    df_municipios = spark.read.parquet(RAW_MUNICIPIOS_PATH)
    
    # Usamos 'cod_municipio' corregido
    df_municipios = df_municipios.select(
        col("cod_municipio").cast("string").alias("codigo_dane_municipio_rds"),
        col("nombre_municipio").alias("nombre_municipio_complementario"),
        col("poblacion_total")
    )

    # Join por código DANE
    df_unificado = df_covid.join(
        df_municipios, 
        df_covid.codigo_dane_municipio == df_municipios.codigo_dane_municipio_rds,
        "left"
    ).drop("codigo_dane_municipio_rds")


    # 4. ESCRITURA EN TRUSTED
    print("Paso 4: Escribiendo resultado en la capa TRUSTED...")

    df_unificado.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(TRUSTED_PATH) 

    print(">>> ETL finalizado correctamente. Datos en Trusted listos. <<<")

except Exception as e:
    print("="*80)
    print("!!! ERROR CRITICO EN LA EJECUCION DEL ETL !!!")
    print(f"La excepción es: {type(e).__name__}")
    print(f"Mensaje: {str(e)}")
    print("="*80)
    raise

spark.stop()