info de la materia: STxxxx <Nombre de la Materia>

Estudiante(s): Gia Mariana Calle, Nathaly Ramirez Henao, John Esteban Duarte, gmcalleh@eafit.edu.co, nramirezh@eafit.edu.co, jeusugad@eafit.edu.co

Profesor: Edwin Nelson Montoya Munera , emontoya@eafit.edu.co

Proyecto: Automatización de un Data Lake de Datos COVID-19 con AWS EMR y Step Functions

1. breve descripción de la actividad

Este proyecto implementa una solución de Ingesta y Procesamiento de Datos para construir un Data Lake basado en la arquitectura de Zonas (Raw, Trusted, Refined) utilizando servicios clave de Amazon Web Services (AWS). El flujo de trabajo está  automatizado a través de AWS Step Functions, que orquesta ejecución de trabajos Spark (ETL y Análisis) y terminación de un clúster de Amazon EMR. Finalmente, los datos procesados son catalogados y consultables usando AWS Athena y Api Gateway.

1.1. Que aspectos cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)

Implementación de Data Lake en S3: Creación y uso de un bucket S3 estructurado en las tres zonas de datos: raw, trusted, y refined.
semi-automatizacion de la ingesta de datos con glue jobs para la ingesta de los datos del ministerio al buket s3/raw y par ala ingesta del rds al buket s3/raw

Procesamiento ETL (Spark): Se desarrollaron y ejecutaron dos trabajos principales de PySpark en EMR:

ETL a Trusted: Limpieza y estandarización inicial de los datos de la zona Raw y persistencia en formato Parquet en la zona Trusted.

Análisis a Refined: Aplicación de transformaciones complejas (cálculo de métricas, uniones con datos de referencia) para generar la tabla de análisis final en formato Parquet en la zona Refined.

Automatización con AWS Step Functions: Creación de una máquina de estados para orquestar los pasos de punta a punta:

Creación de Clúster EMR.
Espera activa por la disponibilidad del clúster.

Adición de los trabajos Spark (ETL y Análisis).

Terminación automática del clúster para optimizar costos.

Seguridad y Permisos (IAM): Configuración detallada y depuración de roles de IAM (Step Functions, EMR Service Role, EMR EC2 Instance Profile) para garantizar la correcta delegación de permisos (iam:PassRole) y la ejecución de las acciones (elasticmapreduce:TerminateJobFlows).

Catálogo de Datos (AWS Athena): Creación de una tabla externa en Athena que apunta a los datos Parquet de la zona Refined, permitiendo consultas SQL ad-hoc.


2. información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas.

El proyecto sigue un patrón de Orquestación de Data Pipeline (Tubería de Datos).

Arquitectura: La solución se basa en una arquitectura de Data Lake en AWS S3 con la metodología de zonas Raw, Trusted, Refined. * Patrones: Se utiliza el patrón ELT (Extract Load Transform), donde los datos se cargan primero a S3 (Raw) y luego se transforman en las capas posteriores.

Mejores Prácticas:

Procesamiento Distribuido: Uso de EMR y Spark para manejar grandes volúmenes de datos de manera eficiente.

Inmutabilidad y Formato: Uso de formato Parquet en las zonas Trusted y Refined para compresión eficiente y optimización de consultas (columnar storage).

Automatización y Ahorro de Costos: El uso de Step Functions garantiza que el clúster EMR se levante solo para procesar los datos y se termine inmediatamente después, evitando cargos innecesarios de EC2.

3. Descripción del ambiente de desarrollo y técnico: lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.

Detalles del Desarrollo

Lenguaje de Programación: Python 3.x

Librerías/Frameworks: Apache Spark (v3.x, implícitamente a través de EMR), Pandas (para uniones locales en PySpark).

como se compila y ejecuta.

El código Spark (emr_etl_staging.py y emr_analisis_final.py) no necesita ser compilado, ya que son scripts de Python. La ejecución se realiza mediante el comando spark-submit dentro del clúster EMR:

spark-submit --deploy-mode cluster s3://<BUCKET_NAME>/scripts/emr_etl_staging.py


detalles técnicos

Entorno Local: IDE con soporte para Python y Spark (ej. VS Code).

Ambiente en Nube (AWS): Ver sección 4.

descripción y como se configura los parámetros del proyecto (ej: ip, puertos, conexión a bases de datos, variables de ambiente, parámetros, etc)

Bucket S3 Principal (búfer de datos): s3://buket-covid-colombia-proyecto3/

Rutas de Entrada/Salida (Scripts PySpark): Las rutas de S3 están codificadas directamente en los scripts, apuntando a las carpetas raw/, trusted/, refined/ dentro del bucket principal.

Parámetros EMR: El Step Function utiliza las instancias m4.xlarge (1 Master, 2 Core) con la versión de EMR emr-7.12.0.

4. Descripción del ambiente de EJECUCIÓN (en producción) lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.

IP o nombres de dominio en nube o en la máquina servidor.

No se utiliza un servidor o IP pública. La ejecución se realiza completamente dentro de la infraestructura de AWS en la región seleccionada (ej. us-east-1):

Step Functions: Orquestador.

EMR: Plataforma de procesamiento.

S3: Almacenamiento y Data Lake.

Athena: Interfaz de consulta.

descripción y como se configura los parámetros del proyecto (ej: ip, puertos, conexión a bases de datos, variables de ambiente, parámetros, etc)

Step Function ARN: arn:aws:states:us-east-1:954976319834:stateMachine:StepFunctions-state-machine-g0y8yxamq (el ARN específico de la Step Function).

Roles de Servicio Clave:

Rol de la Step Function: StepFunctions-state-machine-role-g0y8yxamq

Rol de Servicio EMR: EMR_DefaultRole

Perfil de Instancia EMR EC2: EMR_EC2_DefaultRole

Tiempo de Espera Crítico: El paso Wait for Cluster Ready está configurado con 1000 segundos (16 minutos) para manejar los tiempos de aprovisionamiento de EMR.

como se lanza el servidor.

El proceso se lanza iniciando una nueva ejecución en la consola de AWS Step Functions.

una mini guia de como un usuario utilizaría el software o la aplicación

Carga de Datos: El usuario (o un proceso anterior) debe cargar el archivo de datos sin procesar (.csv) a la ruta s3://buket-covid-colombia-proyecto3/raw/.

Ejecución: Ir a la consola de AWS Step Functions, seleccionar la máquina de estados y hacer clic en "Start Execution".

Monitoreo: Monitorear el progreso en el diagrama de la Step Function hasta que todos los pasos (Create EMR Cluster, Add ETL Trusted Step, etc.) terminen en SUCCEEDED (puede tardar 25-35 minutos).

Consulta de Resultados: Una vez finalizado, ir a AWS Athena y consultar la tabla covid_datalake.casos_covid_refined usando SQL para obtener los datos analíticos.

5. otra información que considere relevante para esta actividad.

Depuración Crítica (Lecciones Aprendidas):
Se encontraron y corrigieron errores clave de seguridad y sintaxis que son comunes en la orquestación de servicios:

Falta de Permiso iam:PassRole: El rol de Step Functions no tenía permitido delegar los roles de EMR.

Error de Sintaxis ARN Duplicado: Se corrigió el ARN de iam:PassRole que tenía la ruta de IAM concatenada dos veces.

Pérdida de Estado: Se añadió "ResultPath": null a los pasos de addStep para preservar el ClusterId y permitir la terminación correcta del clúster.

referencias:

<debemos siempre reconocer los créditos de partes del código que reutilizaremos, así como referencias a youtube, o referencias bibliográficas utilizadas para desarrollar el proyecto o la actividad>

sitio1-url

sitio2-url

url de donde tomo info para desarrollar este proyecto