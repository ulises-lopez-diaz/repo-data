from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, to_timestamp, date_format
from pyspark.sql.functions import current_timestamp, date_format



from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame

import os
from dotenv import load_dotenv
import snowflake.connector
from utils.snowflake_utils import  crear_tabla_en_snowflake_en_base_de_datos_silver
from typing import Optional
import logging

from pyspark.sql.functions import hour, minute


from utils.spark_utils import spark, obtener_datos_de_snowflake



from utils.retail_utils import verificar_directorio, realizar_autenticacion_en_kaggle
from configs.etl_pipeline_silver_configs import sfOptionsSilver, SNOWFLAKE_SOURCE_NAME, snowflake_invoice_silver_connection
from configs.etl_pipeline_bronze_configs import sfOptionsBronzeInvoice
from utils.spark_utils import spark_to_snowflake_type

load_dotenv()




# Crear una instancia de DecimalType con precisión y escala
decimal_type = DecimalType(10, 2)

# def crear_tabla_en_snowflake(create_table_sql):
#     conn = snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
#         database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_SILVER"),
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




def cargar_datos_a_snowflake_a_silver(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_silver) -> bool:
    """
    Carga los DataFrames en Snowflake a la capa Silver.
    
    Parameters:
        diccionario_de_tablas_a_crear_en_la_base_de_datos_de_silver (Dict[str, DataFrame]): Diccionario donde las claves son los nombres de las tablas y los valores son los DataFrames a cargar.
    
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario.
    """

    print("=================== DICCIONARIO DE LAS TABLAS QUE SE VAN A CREAR EN SNOWFLAKE SILVER ==================")


    sql_create_table_invoice_silver_sentence = f"""
                CREATE TABLE IF NOT EXISTS INVOICE_SILVER (
                    INVOICENO STRING,
                    STOCKCODE STRING,
                    DESCRIPTION STRING NOT NULL,
                    QUANTITY DECIMAL(38,0),
                    UNITPRICE DECIMAL(10,2),
                    COUNTRY STRING,
                    FECHA_INVOICE DATE,
                    HORA_MINUTO_VARCHAR STRING,
                    FECHA_DE_CARGA DATE,
                    HORA_DE_CARGA STRING NOT NULL
                );
                """
    
    sql_create_table_invoice_cancelled_silver_sentence = f"""
                CREATE TABLE IF NOT EXISTS INVOICE_CANCELLED_SILVER (
                    INVOICENO STRING,
                    STOCKCODE STRING,
                    DESCRIPTION STRING NOT NULL,
                    QUANTITY DECIMAL(38,0),
                    UNITPRICE DECIMAL(10,2),
                    COUNTRY STRING,
                    FECHA_INVOICE DATE,
                    HORA_MINUTO_VARCHAR STRING,
                    FECHA_DE_CARGA DATE,

                    HORA_DE_CARGA STRING NOT NULL
                );
                """

    lista_de_tablas_necesarias_a_crear = [sql_create_table_invoice_silver_sentence, sql_create_table_invoice_cancelled_silver_sentence]


    for tabla_a_crear_en_base_de_datos_silver in lista_de_tablas_necesarias_a_crear:

        crear_tabla_en_snowflake_en_base_de_datos_silver(tabla_a_crear_en_snowflake=tabla_a_crear_en_base_de_datos_silver)

    for nombre_de_tabla, dataframe_a_cargar_a_snowflake in diccionario_de_tablas_a_crear_en_la_base_de_datos_de_silver.items():
        print(f"Nombre de la tabla: {nombre_de_tabla}")
        print("Esquema del DataFrame:")
        print(dataframe_a_cargar_a_snowflake.printSchema())  
        print("Primeras filas del DataFrame:")
        print(dataframe_a_cargar_a_snowflake.show(10))

        try:
            # Verificar si la tabla existe y crearla si es necesario
            tabla_de_snowflake = obtener_datos_de_snowflake(tabla=nombre_de_tabla, 
                                                            parametrosDeConexion=sfOptionsSilver, 
                                                            seEjecutaEnMainPipeline=False, 
                                                            omitirValidacionTablaVacia=True)
            
            # if tabla_de_snowflake is False and nombre_de_tabla == "INVOICE_SILVER":
            #     print(f"No existe la tabla {nombre_de_tabla} en la base de datos silver de snowflake. Creando la tabla...")
            #     # Ejecutar la sentencia SQL para crear la tabla
                
            #     # Aquí deberías ejecutar el comando SQL en Snowflake usando los métodos apropiados
            #     # connection.execute(sql_create_table)

            if tabla_de_snowflake and nombre_de_tabla == "INVOICE_SILVER":
                print(f"La tabla {nombre_de_tabla} ya existe en la base de datos silver de snowflake.")

                print("==================== INICIO DE INTENTO DE CARGA DE INVOICE SILVER=====================")

                dataframe_a_cargar_invoice_silver = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "INVOICENO", "left_outer") \
                .filter(col("existing_data.INVOICENO").isNull()) \
                .select("new_data.*")

                dataframe_a_cargar_invoice_silver.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptionsSilver) \
                .option("dbtable", nombre_de_tabla) \
                .mode("append") \
                .save()

                print("==================== FIN DE INTENTO DE CARGA DE INVOICE SILVER=====================")
            
            # elif tabla_de_snowflake is False and nombre_de_tabla == "INVOICE_CANCELLED_SILVER":
            #     print(f"No existe la tabla {nombre_de_tabla} en la base de datos silver de snowflake. Creando la tabla...")
            #     # Ejecutar la sentencia SQL para crear la tabla
                

            elif tabla_de_snowflake and nombre_de_tabla == "INVOICE_CANCELLED_SILVER":
                print(f"La tabla {nombre_de_tabla} ya existe en la base de datos silver de snowflake.")

                print("==================== INICIO DE INTENTO DE CARGA DE INVOICE CANCELLED=====================")

                dataframe_a_cargar_invoice_cancelled_silver = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "INVOICENO", "left_outer") \
                .filter(col("existing_data.INVOICENO").isNull()) \
                .select("new_data.*")

                dataframe_a_cargar_invoice_cancelled_silver.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptionsSilver) \
                .option("dbtable", nombre_de_tabla) \
                .mode("append") \
                .save()

                print("==================== FIN DE INTENTO DE CARGA DE INVOICE CANCELLED=====================")

            else:
                pass
        except Exception as exception:
            print(f"Ocurrió un error: {exception}")
            pass

    print("=================== FIN DE LA CARGA DE TABLAS ==================")

    return True
    

def procesar_datos_de_bronze_a_silver(tabla_a_obtener_en_base_de_datos_bronze: str ="INVOICE", 
                                      tablas_en_base_de_datos_a_crear_en_silver: dict= ["INVOICE_SILVER", "INVOICE_SILVER_CANCELACION"]):

    """
    - Se intenta crear una columna de INVOICEDATE perp en formato M/DD/YYYY DATE
    - Se intenta crea una columna de INVOICEHOURS en donde se guarden datos de INVOICEDATE pero solo sus horas timestamp
    - Del dataset a cargar, se quita la columna CustomerID
    - En la columna de descripción, se modifican los none o null
        por textos que digan "SIN_DESCRIPCION"
    - Se dividen los datos, en donde se guardan en otra tabla de cancelacion
      aquellas filas con quantitys negativos y en donde la columna INVOICENO
      empiece con 'C'
    """

    if tabla_a_obtener_en_base_de_datos_bronze == "INVOICE":

        dataframe_de_snowflake_en_bronze = obtener_datos_de_snowflake(tabla= tabla_a_obtener_en_base_de_datos_bronze, parametrosDeConexion=sfOptionsBronzeInvoice, seEjecutaEnMainPipeline=False) 
        lista_de_dataframes_a_cargar_en_snowflake = []
        diccionario_de_dataframes_a_cargar_en_snowflake = {}

        print("INFO DF SNOWFLAKE DE INVOICE BRONZE")
        print(dataframe_de_snowflake_en_bronze.printSchema())
        print(dataframe_de_snowflake_en_bronze.show())

        if dataframe_de_snowflake_en_bronze is not None and dataframe_de_snowflake_en_bronze.head(1):
            # Se definen las columnas requeridas
            columnas_requeridas_de_la_tabla_invoice_en_bronze = ["INVOICENO","STOCKCODE","DESCRIPTION", "QUANTITY", "INVOICEDATE", "UNITPRICE", "CUSTOMERID", "COUNTRY"]


            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.select(columnas_requeridas_de_la_tabla_invoice_en_bronze)


            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                                .withColumn("FECHA_INVOICE", to_date(to_timestamp(dataframe_de_snowflake_en_bronze["INVOICEDATE"], "MM/dd/yyyy HH:mm")))
            
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                                .withColumn("HORA_MINUTO_VARCHAR", date_format(to_timestamp(dataframe_de_snowflake_en_bronze["INVOICEDATE"], "MM/dd/yyyy HH:mm"), "HH:mm"))


            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_bronze["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))
            

            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.dropna(subset=["INVOICENO", "STOCKCODE", "QUANTITY", "UNITPRICE", "COUNTRY"])

            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.na.fill("SIN_DESCRIPCION", subset=["DESCRIPTION"])


            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.drop("CUSTOMERID", "INVOICEDATE", "FECHA_ACTUAL")
            
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                            .withColumn("UNITPRICE", col("UNITPRICE").cast(decimal_type))
            
            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze \
                                            .withColumn("QUANTITY", col("QUANTITY").cast(decimal_type))


            dataframe_de_snowflake_en_bronze_de_cancelaciones = dataframe_de_snowflake_en_bronze

            dataframe_de_snowflake_en_bronze_de_cancelaciones = dataframe_de_snowflake_en_bronze_de_cancelaciones.filter(
                (col("QUANTITY") < 0) & (col("INVOICENO").startswith("C"))
            )

            


            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.drop("INVOICEDATE")


            dataframe_de_snowflake_en_bronze = dataframe_de_snowflake_en_bronze.filter(
                ~((col("QUANTITY") < 0) & (col("INVOICENO").startswith("C")))
            )

            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "INVOICE_SILVER": dataframe_de_snowflake_en_bronze,
                "INVOICE_CANCELLED_SILVER": dataframe_de_snowflake_en_bronze_de_cancelaciones
            }

            cargar_datos_a_snowflake_a_silver(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_silver=diccionario_de_dataframes_a_cargar_en_snowflake)
          