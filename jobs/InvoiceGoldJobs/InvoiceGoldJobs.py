from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, to_timestamp, date_format
from pyspark.sql.functions import current_timestamp, date_format

from pyspark.sql.functions import hour, minute, col, monotonically_increasing_id

from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame



import os
from dotenv import load_dotenv
import snowflake.connector
from utils.snowflake_utils import crear_tabla_en_snowflake_en_base_de_datos_gold
from typing import Optional
import logging

from pyspark.sql.functions import hour, minute

from utils.spark_utils import spark, obtener_datos_de_snowflake


from utils.snowflake_utils import crear_tabla_en_snowflake_en_base_de_datos_gold
from utils.spark_utils import spark

from utils.retail_utils import verificar_directorio, realizar_autenticacion_en_kaggle
from configs.etl_pipeline_silver_configs import sfOptionsSilver
from configs.etl_pipeline_gold_configs import sfOptionsGoldInvoice, SNOWFLAKE_SOURCE_NAME
from utils.spark_utils import spark_to_snowflake_type

load_dotenv()


# Crear una instancia de DecimalType con precisión y escala
decimal_type = DecimalType(10, 2)



def cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold) -> bool:
    """
    Carga los DataFrames en Snowflake a la capa Silver.
    
    Parameters:
        diccionario_de_tablas_a_crear_en_la_base_de_datos_de_silver (Dict[str, DataFrame]): Diccionario donde las claves son los nombres de las tablas y los valores son los DataFrames a cargar.
    
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario.
    """

    print("=================== DICCIONARIO DE LAS TABLAS QUE SE VAN A CREAR EN SNOWFLAKE SILVER ==================")


    sql_create_table_invoice_gold_sentence = f"""
                CREATE TABLE IF NOT EXISTS INVOICE_GOLD (
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
    
    sql_create_table_invoice_date_dim_sentence = f""" 
        CREATE TABLE IF NOT EXISTS INVOICE_DATE_DIM (
            INVOICE_DATE_ID BIGINT AUTOINCREMENT PRIMARY KEY,
            FK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
            FECHA_INVOICE DATE NOT NULL,
            HORA_MINUTO VARCHAR NOT NULL,
            FECHA_DE_CARGA DATE,
            HORA_DE_CARGA STRING NOT NULL
        );
    """


    sql_create_table_country_dim_sentence = f"""
        CREATE TABLE IF NOT EXISTS COUNTRY_DIM (
            COUNTRY_ID BIGINT AUTOINCREMENT PRIMARY KEY,
            FK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
            COUNTRY VARCHAR NOT NULL,
            FECHA_DE_CARGA DATE,
            HORA_DE_CARGA STRING NOT NULL
        );
    """

    
    sql_create_product_dim_sentence = f"""
    
        CREATE TABLE IF NOT EXISTS PRODUCT_DIM (
            PRODUCT_ID BIGINT AUTOINCREMENT PRIMARY KEY,
            FK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
            STOCKCODE VARCHAR NOT NULL,
            FECHA_DE_CARGA DATE,
            HORA_DE_CARGA STRING NOT NULL
        );
    """

    sql_create_invoice_fact_table_sentence = f"""

                CREATE TABLE IF NOT EXISTS INVOICE_FACT_TABLE (
                    INVOICE_FACT_ID BIGINT AUTOINCREMENT PRIMARY KEY,
                    PK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
                    PRODUCT_ID BIGINT REFERENCES PRODUCT_DIM(PRODUCT_ID),
                    COUNTRY_ID BIGINT REFERENCES COUNTRY_DIM(COUNTRY_ID),
                    INVOICE_DATE_ID BIGINT REFERENCES INVOICE_DATE_DIM(INVOICE_DATE_ID),
                    QUANTITY DECIMAL(38, 0),
                    UNITPRICE DECIMAL(10, 2),
                    FECHA_DE_CARGA DATE,
                    HORA_DE_CARGA STRING
                );


            """


    lista_de_tablas_necesarias_a_crear = [sql_create_table_invoice_gold_sentence, 
                                          sql_create_table_invoice_date_dim_sentence,
                                          sql_create_table_country_dim_sentence ,
                                          sql_create_product_dim_sentence,
                                          sql_create_invoice_fact_table_sentence
                                          ]


    for tabla_a_crear_en_base_de_datos_silver in lista_de_tablas_necesarias_a_crear:

        crear_tabla_en_snowflake_en_base_de_datos_gold(tabla_a_crear_en_snowflake=tabla_a_crear_en_base_de_datos_silver)

    for nombre_de_tabla, dataframe_a_cargar_a_snowflake in diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold.items():
        print(f"Nombre de la tabla: {nombre_de_tabla}")
        print("Esquema del DataFrame:")
        print(dataframe_a_cargar_a_snowflake.printSchema())  
        print("Primeras filas del DataFrame:")
        print(dataframe_a_cargar_a_snowflake.show(10))

        try:
            # Verificar si la tabla existe y crearla si es necesario
            tabla_de_snowflake = obtener_datos_de_snowflake(tabla=nombre_de_tabla, 
                                                            parametrosDeConexion=sfOptionsGoldInvoice, 
                                                            seEjecutaEnMainPipeline=False, 
                                                            omitirValidacionTablaVacia=True)
            
            # if tabla_de_snowflake is False and nombre_de_tabla == "INVOICE_SILVER":
            #     print(f"No existe la tabla {nombre_de_tabla} en la base de datos silver de snowflake. Creando la tabla...")
            #     # Ejecutar la sentencia SQL para crear la tabla
                
            #     # Aquí deberías ejecutar el comando SQL en Snowflake usando los métodos apropiados
            #     # connection.execute(sql_create_table)

            if tabla_de_snowflake and nombre_de_tabla == "INVOICE_GOLD":
                print(f"La tabla {nombre_de_tabla} ya existe en la base de datos gold de snowflake.")

                # print("==================== INICIO DE INTENTO DE CARGA DE INVOICE SILVER=====================")

                dataframe_a_cargar_invoice_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "INVOICENO", "left_outer") \
                .filter(col("existing_data.INVOICENO").isNull()) \
                .select("new_data.*")

                dataframe_a_cargar_invoice_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptionsGoldInvoice ) \
                .option("dbtable", nombre_de_tabla) \
                .mode("append") \
                .save()

                # print("==================== FIN DE INTENTO DE CARGA DE INVOICE SILVER=====================")
            
            # elif tabla_de_snowflake is False and nombre_de_tabla == "INVOICE_CANCELLED_SILVER":
            #     print(f"No existe la tabla {nombre_de_tabla} en la base de datos silver de snowflake. Creando la tabla...")
            #     # Ejecutar la sentencia SQL para crear la tabla
                

            elif tabla_de_snowflake and nombre_de_tabla == "INVOICE_CANCELLED_GOLD":
                # print(f"La tabla {nombre_de_tabla} ya existe en la base de datos gold de snowflake.")

                # print("==================== INICIO DE INTENTO DE CARGA DE INVOICE CANCELLED GOLD=====================")

                dataframe_a_cargar_invoice_cancelled_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "INVOICENO", "left_outer") \
                .filter(col("existing_data.INVOICENO").isNull()) \
                .select("new_data.*")

                dataframe_a_cargar_invoice_cancelled_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptionsGoldInvoice) \
                .option("dbtable", nombre_de_tabla) \
                .mode("append") \
                .save()

                # print("==================== FIN DE INTENTO DE CARGA DE INVOICE CANCELLED GOLD=====================")

            
            elif tabla_de_snowflake and nombre_de_tabla == "INVOICE_DATE_DIM":
                # Guardar en Snowflake
                # print("================================== SE INTENTAN CARGAR DATOS A INVOICE_DATE_DIM==================================")


                # Asegúrate de que FK_INVOICENO_STOCKCODE es de tipo String
                dataframe_a_cargar_a_snowflake = dataframe_a_cargar_a_snowflake \
                    .withColumn("FK_INVOICENO_STOCKCODE", col("FK_INVOICENO_STOCKCODE").cast(StringType()))

                # Realiza el join para identificar los nuevos registros
                dataframe_a_cargar_invoice_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "FK_INVOICENO_STOCKCODE", "left_outer") \
                .filter(col("existing_data.FK_INVOICENO_STOCKCODE").isNull()) \
                .select(
                    col("new_data.INVOICE_DATE_ID").alias("INVOICE_DATE_ID"),  # Especifica la tabla de la que proviene la columna
                    col("new_data.FK_INVOICENO_STOCKCODE").alias("FK_INVOICENO_STOCKCODE"),
                    col("new_data.FECHA_INVOICE").alias("FECHA_INVOICE"),
                    col("new_data.HORA_MINUTO").alias("HORA_MINUTO"),
                    col("new_data.FECHA_DE_CARGA").alias("FECHA_DE_CARGA"),
                    col("new_data.HORA_DE_CARGA").alias("HORA_DE_CARGA")
                )


                # # Imprime el esquema y muestra algunos registros para verificar
                # print(dataframe_a_cargar_invoice_gold.printSchema())
                # print(dataframe_a_cargar_invoice_gold.show(10))

                # Escribe los datos en Snowflake especificando la tabla de destino
                dataframe_a_cargar_invoice_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sfOptionsGoldInvoice) \
                    .option("dbtable", nombre_de_tabla) \
                    .mode("append") \
                    .save()
                # print("================================== FIN DE SE INTENTAN CARGAR DATOS A INVOICE_DATE_DIM==================================")

            elif tabla_de_snowflake and nombre_de_tabla == "COUNTRY_DIM":

                # Guardar en Snowflake
                # print("================================== SE INTENTAN CARGAR DATOS A COUNTRY_DIM ==================================")


                # Asegúrate de que FK_INVOICENO_STOCKCODE es de tipo String
                dataframe_a_cargar_a_snowflake = dataframe_a_cargar_a_snowflake \
                    .withColumn("FK_INVOICENO_STOCKCODE", col("FK_INVOICENO_STOCKCODE").cast(StringType()))

                # Realiza el join para identificar los nuevos registros
                dataframe_a_cargar_invoice_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "FK_INVOICENO_STOCKCODE", "left_outer") \
                .filter(col("existing_data.FK_INVOICENO_STOCKCODE").isNull()) \
                .select(
                    col("new_data.COUNTRY_ID").alias("INVOICE_DATE_ID"),  # Especifica la tabla de la que proviene la columna
                    col("new_data.FK_INVOICENO_STOCKCODE").alias("FK_INVOICENO_STOCKCODE"),
                    col("new_data.COUNTRY").alias("COUNTRY"),
                    col("new_data.FECHA_DE_CARGA").alias("FECHA_DE_CARGA"),
                    col("new_data.HORA_DE_CARGA").alias("HORA_DE_CARGA")
                )


                # # Imprime el esquema y muestra algunos registros para verificar
                # print(dataframe_a_cargar_invoice_gold.printSchema())
                # print(dataframe_a_cargar_invoice_gold.show(10))

                # Escribe los datos en Snowflake especificando la tabla de destino
                dataframe_a_cargar_invoice_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sfOptionsGoldInvoice) \
                    .option("dbtable", nombre_de_tabla) \
                    .mode("append") \
                    .save()
                # print("================================== FIN DE SE INTENTAN CARGAR DATOS A COUNTRY_DIM ==================================")

                pass

            elif tabla_de_snowflake and nombre_de_tabla == "PRODUCT_DIM":
                # Guardar en Snowflake
                # print("================================== SE INTENTAN CARGAR DATOS A PRODUCT_DIM ==================================")


                # Asegúrate de que FK_INVOICENO_STOCKCODE es de tipo String
                dataframe_a_cargar_a_snowflake = dataframe_a_cargar_a_snowflake \
                    .withColumn("FK_INVOICENO_STOCKCODE", col("FK_INVOICENO_STOCKCODE").cast(StringType()))

                # Realiza el join para identificar los nuevos registros
                dataframe_a_cargar_invoice_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                .join(tabla_de_snowflake.alias("existing_data"), "FK_INVOICENO_STOCKCODE", "left_outer") \
                .filter(col("existing_data.FK_INVOICENO_STOCKCODE").isNull()) \
                .select(
                    col("new_data.PRODUCT_ID").alias("PRODUCT_ID"),  # Especifica la tabla de la que proviene la columna
                    col("new_data.FK_INVOICENO_STOCKCODE").alias("FK_INVOICENO_STOCKCODE"),
                    col("new_data.STOCKCODE").alias("STOCKCODE"),
                    col("new_data.FECHA_DE_CARGA").alias("FECHA_DE_CARGA"),
                    col("new_data.HORA_DE_CARGA").alias("HORA_DE_CARGA")
                )


                # # Imprime el esquema y muestra algunos registros para verificar
                # print(dataframe_a_cargar_invoice_gold.printSchema())
                # print(dataframe_a_cargar_invoice_gold.show(10))

                # Escribe los datos en Snowflake especificando la tabla de destino
                dataframe_a_cargar_invoice_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sfOptionsGoldInvoice) \
                    .option("dbtable", nombre_de_tabla) \
                    .mode("append") \
                    .save()
                # print("================================== FIN DE SE INTENTAN CARGAR DATOS A PRODUCT_DIMS ==================================")


                pass
            
            elif tabla_de_snowflake and nombre_de_tabla == "INVOICE_FACT_TABLE":

                # Guardar en Snowflake
                print("================================== SE INTENTAN CARGAR DATOS A INVOICE_FACT_TABLE ==================================")


                dataframe_invoice_date_dim_snowflake = obtener_datos_de_snowflake(tabla="INVOICE_DATE_DIM", 
                                                            parametrosDeConexion=sfOptionsGoldInvoice, 
                                                            seEjecutaEnMainPipeline=False, 
                                                            omitirValidacionTablaVacia=True)
                
                dataframe_country_dim_snowflake = obtener_datos_de_snowflake(tabla="COUNTRY_DIM", 
                                                            parametrosDeConexion=sfOptionsGoldInvoice, 
                                                            seEjecutaEnMainPipeline=False, 
                                                            omitirValidacionTablaVacia=True)
                
                dataframe_product_dim_snowflake = obtener_datos_de_snowflake(tabla="PRODUCT_DIM", 
                                                            parametrosDeConexion=sfOptionsGoldInvoice, 
                                                            seEjecutaEnMainPipeline=False, 
                                                            omitirValidacionTablaVacia=True)

                # Asegúrate de que FK_INVOICENO_STOCKCODE es de tipo String
                # TABLA INVOICE_FACT_TABLE
                dataframe_a_cargar_a_snowflake = dataframe_a_cargar_a_snowflake \
                    .withColumn("PK_INVOICENO_STOCKCODE", col("PK_INVOICENO_STOCKCODE").cast(StringType()))

                # Realiza el join para identificar los nuevos registros
                dataframe_a_cargar_invoice_gold = dataframe_a_cargar_a_snowflake.alias("new_data") \
                    .join(tabla_de_snowflake.alias("existing_data"), "PK_INVOICENO_STOCKCODE", "left_outer") \
                    .join(dataframe_invoice_date_dim_snowflake.alias("invoice_date_dim"),
                        col("new_data.PK_INVOICENO_STOCKCODE") == col("invoice_date_dim.FK_INVOICENO_STOCKCODE"), "inner") \
                    .join(dataframe_country_dim_snowflake.alias("country_dim"),
                        col("new_data.PK_INVOICENO_STOCKCODE") == col("country_dim.FK_INVOICENO_STOCKCODE"), "inner") \
                    .join(dataframe_product_dim_snowflake.alias("product_dim"),
                        col("new_data.PK_INVOICENO_STOCKCODE") == col("product_dim.FK_INVOICENO_STOCKCODE"), "inner") \
                    .filter(col("existing_data.PK_INVOICENO_STOCKCODE").isNull()) \
                    .select(
                        col("new_data.INVOICE_FACT_ID").alias("INVOICE_FACT_ID"),
                        col("new_data.PK_INVOICENO_STOCKCODE").alias("PK_INVOICENO_STOCKCODE"),
                        col("product_dim.PRODUCT_ID").alias("PRODUCT_ID"),
                        col("country_dim.COUNTRY_ID").alias("COUNTRY_ID"),
                        col("invoice_date_dim.INVOICE_DATE_ID").alias("INVOICE_DATE_ID"),
                        col("new_data.QUANTITY").alias("QUANTITY"),
                        col("new_data.UNITPRICE").alias("UNITPRICE"),
                        col("new_data.FECHA_DE_CARGA").alias("FECHA_DE_CARGA"),
                        col("new_data.HORA_DE_CARGA").alias("HORA_DE_CARGA")
                    ).distinct()


                # Imprime el esquema y muestra algunos registros para verificar
                print(dataframe_a_cargar_invoice_gold.printSchema())
                print(dataframe_a_cargar_invoice_gold.show(10))
                print(dataframe_a_cargar_invoice_gold.count())
                #Escribe los datos en Snowflake especificando la tabla de destino
                dataframe_a_cargar_invoice_gold.write.format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sfOptionsGoldInvoice) \
                    .option("dbtable", nombre_de_tabla) \
                    .mode("append") \
                    .save()
                print("================================== FIN DE SE INTENTAN CARGAR DATOS A INVOICE_FACT_TABLE ==================================")
                pass


            else:
                pass
        except Exception as exception:
            print(f"Ocurrió un error: {exception}")
            pass

    print("=================== FIN DE LA CARGA DE TABLAS ==================")

    return True
    

def procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver: str = None, 
                                      tablas_en_base_de_datos_a_cargar_en_gold: str = None):

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
    dataframe_de_snowflake_en_silver= obtener_datos_de_snowflake(tabla= tabla_a_obtener_en_base_de_datos_silver, parametrosDeConexion=sfOptionsSilver, seEjecutaEnMainPipeline=False) 
    lista_de_dataframes_a_cargar_en_snowflake = []
    diccionario_de_dataframes_a_cargar_en_snowflake = {}

    if tablas_en_base_de_datos_a_cargar_en_gold== "INVOICE_GOLD":

      
        print("INFO DF SNOWFLAKE DE INVOICE SILVER")
        print(dataframe_de_snowflake_en_silver.printSchema())
        print(dataframe_de_snowflake_en_silver.show())

        if dataframe_de_snowflake_en_silver is not None and dataframe_de_snowflake_en_silver.head(1):
            # Se definen las columnas requeridas
            columnas_requeridas_de_la_tabla_invoice_en_silver = ["INVOICENO",
                                                                 "STOCKCODE",
                                                                 "DESCRIPTION", 
                                                                 "QUANTITY", 
                                                                 "UNITPRICE",
                                                                 "COUNTRY",
                                                                 "FECHA_INVOICE",
                                                                 "HORA_MINUTO_VARCHAR"]


            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(columnas_requeridas_de_la_tabla_invoice_en_silver)


            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
            #                                     .withColumn("FECHA_INVOICE", to_date(to_timestamp(dataframe_de_snowflake_en_silver["INVOICEDATE"], "MM/dd/yyyy HH:mm")))
            
            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
            #                                     .withColumn("HORA_MINUTO_VARCHAR", date_format(to_timestamp(dataframe_de_snowflake_en_silver["INVOICEDATE"], "MM/dd/yyyy HH:mm"), "HH:mm"))


            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_silver["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))
            

            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.dropna(subset=["INVOICENO", "STOCKCODE", "QUANTITY", "UNITPRICE", "COUNTRY"])

            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.na.fill("SIN_DESCRIPCION", subset=["DESCRIPTION"])


            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.drop("FECHA_ACTUAL")
            
            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
            #                                 .withColumn("UNITPRICE", col("UNITPRICE").cast(decimal_type))
            
            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
            #                                 .withColumn("QUANTITY", col("QUANTITY").cast(decimal_type))


            # dataframe_de_snowflake_en_bronze_de_cancelaciones = dataframe_de_snowflake_en_bronze

            # dataframe_de_snowflake_en_bronze_de_cancelaciones = dataframe_de_snowflake_en_bronze_de_cancelaciones.filter(
            #     (col("QUANTITY") < 0) & (col("INVOICENO").startswith("C"))
            # )

            


            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.drop("INVOICEDATE")


            # dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.filter(
            #     ~((col("QUANTITY") < 0) & (col("INVOICENO").startswith("C")))
            # )

            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "INVOICE_GOLD": dataframe_de_snowflake_en_silver
            }

            cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold=diccionario_de_dataframes_a_cargar_en_snowflake)
        
    
    if tablas_en_base_de_datos_a_cargar_en_gold== "INVOICE_DATE_DIM":
        if dataframe_de_snowflake_en_silver is not None and dataframe_de_snowflake_en_silver.head(1):
            # Obtener la columna de fecha y hora desde la tabla de hechos
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("FECHA_INVOICE").alias("FECHA_INVOICE"),
                col("HORA_MINUTO_VARCHAR").alias("HORA_MINUTO"),
                col("INVOICENO"),
                col("STOCKCODE")
            )

            # Agregar un ID único (opcional si quieres controlar el ID en lugar de usar el autoincrement de Snowflake)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn("INVOICE_DATE_ID", monotonically_increasing_id())

            # Crear la columna "FK_INVOICENO_STOCKCODE" concatenando "INVOICENO" y "STOCKCODE", y convertirla a VARCHAR (StringType)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn(
                "FK_INVOICENO_STOCKCODE", 
                F.concat(F.col("INVOICENO").cast(StringType()), F.lit("_"), F.col("STOCKCODE")).cast(StringType())
            )

            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_silver["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))

            # Seleccionar las columnas requeridas
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("INVOICE_DATE_ID"), 
                col("FK_INVOICENO_STOCKCODE"), 
                col("FECHA_INVOICE"), 
                col("HORA_MINUTO"),
                col("FECHA_DE_CARGA"),
                col("HORA_DE_CARGA")
            )

            print("========================== INVOCIE_DATE_DIM =================================")
            print(dataframe_de_snowflake_en_silver.printSchema())
            print(dataframe_de_snowflake_en_silver.show(10))
                
            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "INVOICE_DATE_DIM": dataframe_de_snowflake_en_silver
            }

        cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold=diccionario_de_dataframes_a_cargar_en_snowflake)
    if tablas_en_base_de_datos_a_cargar_en_gold== "COUNTRY_DIM":

    #     f"""
    #     CREATE TABLE IF NOT EXISTS COUNTRY_DIM (
    #         COUNTRY_ID BIGINT AUTOINCREMENT PRIMARY KEY,
    #         FK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
    #         COUNTRY VARCHAR NOT NULL,
    #         FECHA_DE_CARGA DATE,
    #         HORA_DE_CARGA STRING NOT NULL
    #     );
    # """

        if dataframe_de_snowflake_en_silver is not None and dataframe_de_snowflake_en_silver.head(1):
            # Obtener la columna de fecha y hora desde la tabla de hechos
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("COUNTRY"),
                col("INVOICENO"),
                col("STOCKCODE")
            )

            # Agregar un ID único (opcional si quieres controlar el ID en lugar de usar el autoincrement de Snowflake)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn("COUNTRY_ID", monotonically_increasing_id())

            # Crear la columna "FK_INVOICENO_STOCKCODE" concatenando "INVOICENO" y "STOCKCODE", y convertirla a VARCHAR (StringType)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn(
                "FK_INVOICENO_STOCKCODE", 
                F.concat(F.col("INVOICENO").cast(StringType()), F.lit("_"), F.col("STOCKCODE")).cast(StringType())
            )

            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_silver["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))

            # Seleccionar las columnas requeridas
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("COUNTRY_ID"), 
                col("FK_INVOICENO_STOCKCODE"), 
                col("COUNTRY"),
                col("FECHA_DE_CARGA"),
                col("HORA_DE_CARGA")
            )

            print("========================== COUNTRY_DIM =================================")
            print(dataframe_de_snowflake_en_silver.printSchema())
            print(dataframe_de_snowflake_en_silver.show(10))
                
            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "COUNTRY_DIM": dataframe_de_snowflake_en_silver
            }

            cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold=diccionario_de_dataframes_a_cargar_en_snowflake)

    if tablas_en_base_de_datos_a_cargar_en_gold == "PRODUCT_DIM":

    #     f"""
    
    #     CREATE TABLE IF NOT EXISTS PRODUCT_DIM (
    #         PRODUCT_ID BIGINT AUTOINCREMENT PRIMARY KEY,
    #         FK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
    #         STOCKCODE VARCHAR NOT NULL,
    #         DESCRIPTION VARCHAR NOT NULL,
    #         FECHA_DE_CARGA DATE,
    #         HORA_DE_CARGA STRING NOT NULL
    #     );
    # """
        
        if dataframe_de_snowflake_en_silver is not None and dataframe_de_snowflake_en_silver.head(1):
            # Obtener la columna de fecha y hora desde la tabla de hechos
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("DESCRIPTION"),
                col("INVOICENO"),
                col("STOCKCODE")
            )

            # Agregar un ID único (opcional si quieres controlar el ID en lugar de usar el autoincrement de Snowflake)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn("PRODUCT_ID", monotonically_increasing_id())

            # Crear la columna "FK_INVOICENO_STOCKCODE" concatenando "INVOICENO" y "STOCKCODE", y convertirla a VARCHAR (StringType)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn(
                "FK_INVOICENO_STOCKCODE", 
                F.concat(F.col("INVOICENO").cast(StringType()), F.lit("_"), F.col("STOCKCODE")).cast(StringType())
            )

            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_silver["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))

            # Seleccionar las columnas requeridas
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("PRODUCT_ID"), 
                col("FK_INVOICENO_STOCKCODE"), 
                col("STOCKCODE"), 
                col("FECHA_DE_CARGA"),
                col("HORA_DE_CARGA")
            )

            print("========================== PRODUCT_DIM =================================")
            print(dataframe_de_snowflake_en_silver.printSchema())
            print(dataframe_de_snowflake_en_silver.show(10))
                
            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "PRODUCT_DIM": dataframe_de_snowflake_en_silver
            }

        cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold=diccionario_de_dataframes_a_cargar_en_snowflake)
    
    if tablas_en_base_de_datos_a_cargar_en_gold == "INVOICE_FACT_TABLE":


        # f"""

        #         CREATE TABLE IF NOT EXISTS INVOICE_FACT_TABLE (
        #             INVOICE_FACT_ID BIGINT AUTOINCREMENT PRIMARY KEY,
        #             PK_INVOICENO_STOCKCODE VARCHAR NOT NULL,
        #             PRODUCT_ID BIGINT REFERENCES PRODUCT_DIM(PRODUCT_ID),
        #             COUNTRY_ID BIGINT REFERENCES COUNTRY_DIM(COUNTRY_ID),
        #             INVOICE_DATE_ID BIGINT REFERENCES INVOICE_DATE_DIM(INVOICE_DATE_ID),
        #             QUANTITY DECIMAL(38, 0),
        #             UNITPRICE DECIMAL(10, 2),
        #             FECHA_DE_CARGA DATE,
        #             HORA_DE_CARGA STRING
        #         );


        #     """


        if dataframe_de_snowflake_en_silver is not None and dataframe_de_snowflake_en_silver.head(1):
            # Obtener la columna de fecha y hora desde la tabla de hechos
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("COUNTRY"),
                col("INVOICENO"),
                col("STOCKCODE"),
                col("QUANTITY"),
                col("UNITPRICE")
            )

            # Agregar un ID único (opcional si quieres controlar el ID en lugar de usar el autoincrement de Snowflake)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn("INVOICE_FACT_ID", monotonically_increasing_id())

            # Crear la columna "FK_INVOICENO_STOCKCODE" concatenando "INVOICENO" y "STOCKCODE", y convertirla a VARCHAR (StringType)
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.withColumn(
                "PK_INVOICENO_STOCKCODE", 
                F.concat(F.col("INVOICENO").cast(StringType()), F.lit("_"), F.col("STOCKCODE")).cast(StringType())
            )

            # Agregar la columna con la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_ACTUAL", current_timestamp())
            
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                                                .withColumn("FECHA_DE_CARGA", to_date(to_timestamp(dataframe_de_snowflake_en_silver["FECHA_ACTUAL"], "MM/dd/yyyy HH:mm")))
            

            # Opcional: Extraer solo la hora y minutos de la hora actual
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver \
                .withColumn("HORA_DE_CARGA", date_format("FECHA_ACTUAL", "HH:mm"))

            # Seleccionar las columnas requeridas
            dataframe_de_snowflake_en_silver = dataframe_de_snowflake_en_silver.select(
                col("INVOICE_FACT_ID"), 
                col("PK_INVOICENO_STOCKCODE"), 
                col("QUANTITY"),
                col("UNITPRICE"),
                col("FECHA_DE_CARGA"),
                col("HORA_DE_CARGA")
            )

            print("========================== COUNTRY_DIM =================================")
            print(dataframe_de_snowflake_en_silver.printSchema())
            print(dataframe_de_snowflake_en_silver.show(10))
                
            diccionario_de_dataframes_a_cargar_en_snowflake = {
                "INVOICE_FACT_TABLE": dataframe_de_snowflake_en_silver
            }

            cargar_datos_a_snowflake_a_gold(diccionario_de_tablas_a_crear_en_la_base_de_datos_de_gold=diccionario_de_dataframes_a_cargar_en_snowflake)

    # else:
    #     pass