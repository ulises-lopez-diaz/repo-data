from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType, DecimalType
from typing import Dict, Optional
from configs.etl_pipeline_bronze_configs import SNOWFLAKE_SOURCE_NAME

import logging


spark = SparkSession.builder \
    .appName("SnowflakeDataRead") \
    .config("spark.jars", "jobs/jar_files/spark-snowflake_2.12-2.16.0-spark_3.4.jar, jobs/jar_files/snowflake-jdbc-3.16.1.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def spark_to_snowflake_type(spark_type):
    if isinstance(spark_type, StringType):
        return "STRING"
    elif isinstance(spark_type, IntegerType):
        return "NUMBER"
    elif isinstance(spark_type, FloatType):
        return "FLOAT"
    elif isinstance(spark_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(spark_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(spark_type, DecimalType):
        return "NUMBER"  # Adjust as needed for precision
    else:
        return "STRING"
    
def obtener_datos_de_snowflake(tabla: str = None, 
                               parametrosDeConexion: dict = {}, 
                               seEjecutaEnMainPipeline: bool = False, omitirValidacionTablaVacia=False) -> Optional[DataFrame]:
    """
    Obtiene datos de una tabla en Snowflake y los carga en un DataFrame de Spark.

    Args:
        tabla (str): Nombre de la tabla en Snowflake. Por defecto es None.
        parametrosDeConexion (dict): Parámetros de conexión a Snowflake.
        seEjecutaEnMainPipeline (bool): Indica si se está ejecutando en el pipeline principal.

    Returns:
        Optional[pyspark.sql.DataFrame]: DataFrame de Spark con los datos de la tabla, 
        o None si ocurre un error o la tabla está vacía.
    """

    if seEjecutaEnMainPipeline:
        logging.info("Se está ejecutando el proceso para obtener datos de Snowflake")
    else:
        if not tabla:
            logging.error("No se indicó la tabla a la que se realizará la conexión")
            return None

        if not parametrosDeConexion:
            logging.error("No se recibieron los parámetros de conexión al intentar obtener el dataset")
            return None

        try:
            dataframe_de_spark = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**parametrosDeConexion) \
                .option("dbtable", tabla) \
                .load()
            
            if not omitirValidacionTablaVacia:
                # Verificar si el DataFrame tiene datos de manera eficiente
                if dataframe_de_spark.head(1):
                    return dataframe_de_spark
                else:
                    logging.warning(f"La tabla {tabla} está vacía.")
                    return None
            else:
                return dataframe_de_spark
            
        except Exception as e:
            logging.error(f"Ocurrió un error al intentar obtener datos del dataset {tabla}: {e}")
            # Si no se encontro la tabla, se regresa un falso indicando que no se encontro la tabla que se intento obtener en snowflake
            return False