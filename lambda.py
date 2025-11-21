import json
import os
import boto3
import pandas as pd
import io


# CONFIGURACIÓN IMPORTANTE

S3_BUCKET = "buket-covid-colombia-proyecto3"
# Ruta a los datos del resumen departamental (capa refined)
S3_KEY_PREFIX = "refined/resumen_analitico/resumen_departamental/" 


# Inicializar S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Función Lambda que lee todos los archivos Parquet en la ruta Refined 
    y devuelve el resumen departamental en formato JSON para la API.
    
    Esta función requiere que la capa con Pandas y PyArrow esté adjunta.
    """
    try:
        # 1. Listar todos los archivos Parquet en la ruta
        print(f"Buscando archivos en s3://{S3_BUCKET}/{S3_KEY_PREFIX}")
        # La función list_objects_v2 es más eficiente para listar archivos grandes
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_KEY_PREFIX)
        
        # Filtramos solo los archivos Parquet (que son los que queremos leer)
        parquet_files = [
            obj['Key'] for obj in response.get('Contents', []) 
            if obj['Key'].endswith('.parquet') and obj['Size'] > 0
        ]
        
        if not parquet_files:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontraron datos Parquet para el resumen departamental.'})
            }

        # 2. Leer los archivos Parquet en Pandas
        all_data = []
        for key in parquet_files:
            s3_object = s3.get_object(Bucket=S3_BUCKET, Key=key)
            # Leer el contenido del objeto S3 directamente en un búfer de memoria (io.BytesIO)
            # para que pandas lo procese sin necesidad de guardarlo en /tmp
            df = pd.read_parquet(io.BytesIO(s3_object['Body'].read()))
            all_data.append(df)
            
        # 3. Concatenar y convertir a JSON
        final_df = pd.concat(all_data, ignore_index=True)
        # Rellenamos NaNs (valores nulos de números) con 0 para evitar errores de serialización JSON
        final_df = final_df.fillna(0) 
        # Convertimos a lista de diccionarios, que es el formato de respuesta ideal para una API REST
        results = final_df.to_dict('records')
        
        print(f"Resumen de {len(results)} registros listo para el API.")

        # Devolver la respuesta HTTP 200 con los datos JSON
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                # Este encabezado es crucial para que navegadores externos puedan acceder a la API (CORS)
                'Access-Control-Allow-Origin': '*' 
            },
            'body': json.dumps(results, indent=2)
        }
        
    except Exception as e:
        print(f"Error en Lambda: {str(e)}")
        # Devolver un error JSON en caso de fallo
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': 'Error interno del servidor.', 'detail': str(e)})
        }