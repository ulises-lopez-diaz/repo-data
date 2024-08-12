import os
import logging
import snowflake.connector


def crear_tabla_en_snowflake_en_base_de_datos_bronze(tabla_a_crear_en_snowflake: str = None, conexion_a_snowflake: snowflake.connector = None):

    if conexion_a_snowflake is None:
        logging.error(f"No se recibio la conexion a snowflake {conexion_a_snowflake}")
    # conn = snowflake.connector.connect(
    #     user=os.getenv("SNOWFLAKE_USER"),
    #     password=os.getenv("SNOWFLAKE_PASSWORD"),
    #     account=os.getenv("SNOWFLAKE_ACCOUNT"),
    #     warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
    #     database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_BRONZE"),
    #     schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    # )


    if tabla_a_crear_en_snowflake is None:
        logging.error(f"No se recibio el nombre de la tabla a crear en snoflake, el valor que se recibio es '{tabla_a_crear_en_snowflake}'")
        return

    try:
        
        cur = conexion_a_snowflake.cursor()
        cur.execute(tabla_a_crear_en_snowflake)
        print(f"Tabla creada exitosamente: {tabla_a_crear_en_snowflake}")
    except Exception as exception:
        print(f"Error al crear la tabla: {exception}")
    finally:
        cur.close()
        conexion_a_snowflake.close()


def crear_tabla_en_snowflake_en_base_de_datos_silver(tabla_a_crear_en_snowflake: str = None):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
        database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_SILVER"),
        schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    )
    try:
        
        cur = conn.cursor()
        cur.execute(tabla_a_crear_en_snowflake)
        print(f"Tabla creada exitosamente: {tabla_a_crear_en_snowflake}")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")
    finally:
        cur.close()
        conn.close()

def crear_tabla_en_snowflake_en_base_de_datos_gold(tabla_a_crear_en_snowflake: str = None):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
        database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_GOLD"),
        schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    )
    try:
        
        cur = conn.cursor()
        cur.execute(tabla_a_crear_en_snowflake)
        print(f"Tabla creada exitosamente: {tabla_a_crear_en_snowflake}")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")
    finally:
        cur.close()
        conn.close()



# def crear_tabla_en_snowflake(create_table_sql):
#     conn = snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
#         database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_GOLD"),
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
