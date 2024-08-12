from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType
import os
from dotenv import load_dotenv
import snowflake.connector
from configs.etl_pipeline_bronze_configs import snowflake_invoice_bronze_connection

import kaggle

from utils.retail_utils import verificar_directorio, realizar_autenticacion_en_kaggle
from utils.snowflake_utils import crear_tabla_en_snowflake_en_base_de_datos_bronze
from configs.etl_pipeline_bronze_configs import sfOptionsBronzeInvoice, SNOWFLAKE_SOURCE_NAME
from utils.spark_utils import spark_to_snowflake_type

load_dotenv()

spark = SparkSession.builder \
    .appName("SnowflakeDataRead") \
    .config("spark.jars", "jobs/jar_files/spark-snowflake_2.12-2.16.0-spark_3.4.jar, jobs/jar_files/snowflake-jdbc-3.16.1.jar") \
    .getOrCreate()

# # Snowflake connection options
# SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# sfOptions = {
#     "sfURL": os.getenv("SNOWFLAKE_URL"),
#     "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
#     "sfUser": os.getenv("SNOWFLAKE_USER"),
#     "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
#     "sfDatabase": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_BRONZE"),
#     "sfSchema": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"),
#     "sfWarehouse": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
#     "sfRole": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE")
# }

# def spark_to_snowflake_type(spark_type):
#     if isinstance(spark_type, StringType):
#         return "STRING"
#     elif isinstance(spark_type, IntegerType):
#         return "NUMBER"
#     elif isinstance(spark_type, FloatType):
#         return "FLOAT"
#     elif isinstance(spark_type, BooleanType):
#         return "BOOLEAN"
#     elif isinstance(spark_type, TimestampType):
#         return "TIMESTAMP"
#     else:
#         return "STRING"

def obtener_esquema_tabla_snowflake(tabla):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
        database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_BRONZE"),
        schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    )
    try:
        cur = conn.cursor()
        cur.execute(f"DESC TABLE {tabla}")
        columns = cur.fetchall()
        return {col[0]: col[1] for col in columns}
    except Exception as e:
        print(f"Error al obtener el esquema de la tabla: {e}")
        return {}
    finally:
        cur.close()
        conn.close()

def actualizar_esquema_tabla_snowflake(tabla, columnas_nuevas):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
        database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_BRONZE"),
        schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    )
    try:
        cur = conn.cursor()
        for columna in columnas_nuevas:
            # Validar y construir la instrucción SQL
            columna_sql = f'"{columna.split()[0]}" {columna.split()[1]}'
            cur.execute(f"ALTER TABLE {tabla} ADD COLUMN {columna_sql}")
        print(f"Se añadieron las columnas nuevas a la tabla '{tabla}'.")
    except Exception as e:
        print(f"Error al actualizar el esquema de la tabla: {e}")
    finally:
        cur.close()
        conn.close()

# def crear_tabla_en_snowflake(create_table_sql):
#     conn = snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
#         database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_BRONZE"),
#         schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
#     )
#     try:
        
#         cur = conn.cursor()
#         cur.execute(create_table_sql)
#         print(f"Tabla creada exitosamente: {create_table_sql}")
#     except Exception as e:
#         print(f"Error al crear la tabla: {e}")
#     finally:
#         cur.close()
#         conn.close()

def obtener_datos_de_kaggle(dataset="vijayuv/onlineretail", path="data/bronze"):
    try:
        realizar_autenticacion_en_kaggle()
        verificar_directorio()
        kaggle.api.dataset_download_files(dataset, path, unzip=True)
        pass
    except Exception as e:
        print(f"Ocurrió un error al obtener los datos de Kaggle: {e}")
    return "Se obtuvieron los datos"

def cargar_datos_en_snowflake_data_warehouse_bronze(ruta_dataset="data/bronze", tabla="INVOICE"):
    df_online_retail_csv = spark.read.csv(ruta_dataset, header=True, inferSchema=True)

    schema_df = df_online_retail_csv.schema
    columnas_actuales = {field.name: spark_to_snowflake_type(field.dataType) for field in schema_df.fields}

    # try:

    #     # Eliminar las columnas DESCRIPTION y COUNTRY
    #     df_online_retail_csv = df_online_retail_csv.drop("DESCRIPTION", "COUNTRY")
    # except:
    #     pass

    try:
        # Leer los datos existentes en Snowflake
        df_snowflake = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptionsBronzeInvoice) \
            .option("dbtable", tabla) \
            .load()

        # Verificar si hay datos en Snowflake
        if df_snowflake.count() > 0:
            # Asumimos que la columna que define unicidad es 'INVOICENO'; 
            df_online_retail_csv = df_online_retail_csv.alias("new_data") \
                .join(df_snowflake.alias("existing_data"), "INVOICENO", "left_outer") \
                .filter(col("existing_data.INVOICENO").isNull()) \
                .select("new_data.*")

            # Obtener el esquema actual de Snowflake
            esquema_snowflake = obtener_esquema_tabla_snowflake(tabla)

            # Comparar esquemas
            columnas_nuevas = []
            columnas_eliminadas_en_dataset_de_kaggle = []
            for columna, tipo in columnas_actuales.items():
                if columna.upper() not in esquema_snowflake:
                    columnas_nuevas.append(f"{columna} {tipo}")

            if columnas_nuevas:
                # Actualizar la tabla en Snowflake
                actualizar_esquema_tabla_snowflake(tabla, columnas_nuevas)

            # Cargar los datos filtrados en Snowflake
            df_online_retail_csv.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptionsBronzeInvoice) \
                .option("dbtable", tabla) \
                .mode("append") \
                .save()
        

        print(f"Datos cargados en la tabla '{tabla}' en Snowflake.")

    except Exception as e:
        print(f"Error al cargar los datos en Snowflake: {e}")
        # En caso de error, podrías intentar crear la tabla y cargar los datos si aún no existe
        schema_sql = ', '.join([f"{columna} {spark_to_snowflake_type(schema_df[columna].dataType)}" for columna in columnas_actuales])
        create_table_sql = f"CREATE TABLE {tabla} ({schema_sql})"
        crear_tabla_en_snowflake_en_base_de_datos_bronze(tabla_a_crear_en_snowflake=tabla, conexion_a_snowflake=snowflake_invoice_bronze_connection)
        df_online_retail_csv.write.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptionsBronzeInvoice) \
            .option("dbtable", tabla) \
            .mode("append") \
            .save()
        print(f"Tabla '{tabla}' creada y datos cargados en Snowflake.")

# Función para guardar los datos en una carpeta local
def guardar_datos_en_carpeta_local(nombre_carpeta="", ubicacion_carpeta=""):
    return "Se guardaron los datos"
