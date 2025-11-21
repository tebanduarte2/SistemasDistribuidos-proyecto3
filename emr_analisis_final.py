from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, current_date, datediff, avg, coalesce, to_date, when

# Inicializar Spark Session
spark = SparkSession.builder.appName("Analitica_Covid_Refined_V14").getOrCreate()


# CONFIGURACION DE RUTAS (DEFINICION CLARA PARA EVITAR NameError)

TRUSTED_PATH = "s3://buket-covid-colombia-proyecto3/trusted/covid_unificado/" 
REFINED_PATH = "s3://buket-covid-colombia-proyecto3/refined/resumen_analitico/"



# 1. LECTURA DE DATOS TRUSTED

print("Paso 1: Leyendo datos limpios y unificados de la zona TRUSTED...")
try:
    df_trusted = spark.read.parquet(TRUSTED_PATH)
except Exception as e:
    print(f"!!! ERROR: No se pudieron leer los datos de Trusted. Asegúrate que la ruta ({TRUSTED_PATH}) tenga archivos. Mensaje: {str(e)}")
    # Si la ruta no existe, es posible que el Step ETL (V12) no se haya ejecutado primero.
    raise

conteo_trusted = df_trusted.count()
if conteo_trusted == 0:
    print("!!! ADVERTENCIA: DataFrame Trusted está vacío (0 filas). El resultado analítico será 0.")
else:
    print(f"Se leyeron {conteo_trusted} registros de la capa Trusted.")



# 2. ANALÍTICA DESCRIPTIVA AVANZADA (Lab 3-4 Requerimientos)

print("Paso 2: Realizando Análisis Descriptivo (Requerimientos del Lab 3-4)...")

# --- A. Duración Promedio de la Enfermedad y Casos (DataFrames Pipeline) ---
# Usamos 'fecha_recuperacion' y 'fecha_diagnostico' del dataset.
df_duracion = df_trusted.withColumn(
    # Convertir a fecha, tratando posibles nulos
    "fecha_recuperacion_dt", to_date(col("fecha_recuperacion")) 
).withColumn(
    "fecha_diagnostico_dt", to_date(col("fecha_diagnostico"))   
).withColumn(
    # Calcular la diferencia de días; si alguna fecha es nula, el resultado es nulo
    "dias_enfermedad", datediff(col("fecha_recuperacion_dt"), col("fecha_diagnostico_dt"))
).withColumn(
    # Filtrar solo casos recuperados para el promedio de duración
    "dias_validos", when(col("recuperado") == "Recuperado", col("dias_enfermedad")).otherwise(None)
)

# Agregación principal: Casos, Edad y Días Promedio por Departamento
df_analisis_final = df_duracion.groupBy(
    col("nombre_departamento")
).agg(
    count(lit(1)).alias("total_casos_por_depto"),
    avg(col("edad")).alias("edad_promedio_depto"),
    # Usamos coalesce para que si el promedio es nulo (división por cero), devuelva 0
    coalesce(avg(col("dias_validos")), lit(0)).alias("dias_promedio_recuperacion")
).sort(col("total_casos_por_depto").desc())

# --- B. Agrupación por Sexo y Estado (Similar a Lab 3-4) ---
df_sexo_estado = df_trusted.groupBy(
    col("sexo"), col("estado")
).agg(
    count(lit(1)).alias("total_casos")
).sort(col("total_casos").desc())


# 3. ESCRITURA DE RESULTADOS A REFINED

print("Paso 3: Escribiendo resultados a la zona Refined...")

# 3.1. Escribir el resumen principal (Casos totales, Edad y Duración promedio)
df_analisis_final.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(f"{REFINED_PATH}resumen_departamental/")

# 3.2. Escribir el resumen por Sexo y Estado
df_sexo_estado.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(f"{REFINED_PATH}sexo_estado/")


print(">>> Analítica finalizada correctamente. Datos en Refined listos. <<<")
spark.stop()