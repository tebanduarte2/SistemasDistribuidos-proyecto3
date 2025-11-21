from pyspark.sql import SparkSession

# Inicializar
spark = SparkSession.builder.appName("ETL_Debug_Columns").getOrCreate()

RAW_COVID_PATH = "s3://buket-covid-colombia-proyecto3/raw/covid/"

print(">>> INICIANDO DIAGNOSTICO V5 <<<")

try:
    # 1. Leemos SIN tocar nada, confiando en el header del archivo
    print("Leyendo CSV...")
    df_raw = spark.read.csv(
        RAW_COVID_PATH, 
        header=True,       
        inferSchema=False, # Rapido
        sep=','            
    )
    
    # 2. IMPRIMIMOS LA VERDAD
    print("="*50)
    print(f"CANTIDAD DE COLUMNAS DETECTADAS: {len(df_raw.columns)}")
    print("="*50)
    
    print("LISTA DE COLUMNAS QUE VE SPARK:")
    print(df_raw.columns)
    print("="*50)

    # 3. INTENTAMOS VER UNA FILA
    # Si esto falla, el archivo esta corrupto.
    print("Muestra de 1 registro:")
    df_raw.show(1, vertical=True)
    
    print(">>> DIAGNOSTICO COMPLETADO EXITOSAMENTE <<<")

except Exception as e:
    print("!!!"*20)
    print(f"ERROR CRITICO EN EL DIAGNOSTICO: {str(e)}")
    print("!!!"*20)

spark.stop()