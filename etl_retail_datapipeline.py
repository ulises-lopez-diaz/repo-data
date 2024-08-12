from jobs.etl_pipeline_invoice_bronze import obtener_datos_de_kaggle, guardar_datos_en_carpeta_local, cargar_datos_en_snowflake_data_warehouse_bronze
from jobs.InvoiceSilverJobs.InvoiceSilverJobs import obtener_datos_de_snowflake, procesar_datos_de_bronze_a_silver
from jobs.InvoiceGoldJobs.InvoiceGoldJobs import procesar_datos_de_silver_a_gold, cargar_datos_a_snowflake_a_gold
from prefect import flow, task
import logging

@task
def extraer_datos():
    datos = obtener_datos_de_kaggle()
    return(datos)


@task
def guardar_datos():
    respuestaGuardado = guardar_datos_en_carpeta_local()
    print(respuestaGuardado)

@task
def cargar_datos_bronze_task():
    cargar_datos_en_snowflake_data_warehouse_bronze()

@task
def obtener_datos_de_snowflake_para_el_dataset_invoice_silver():
    obtener_datos_de_snowflake(tabla = None, parametrosDeConexion = {}, seEjecutaEnMainPipeline= True)

@task
def procesar_datos_de_bronze_a_silver_tabla_invoice():
    procesar_datos_de_bronze_a_silver(tabla_a_obtener_en_base_de_datos_bronze="INVOICE", 
                                      tablas_en_base_de_datos_a_crear_en_silver=["INVOICE_SILVER", "INVOICE_CANCELLED_SILVER"])

@task
def task_procesar_datos_de_silver_a_gold():
    procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
                                    tablas_en_base_de_datos_a_cargar_en_gold="INVOICE_GOLD")
    
    procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
                                    tablas_en_base_de_datos_a_cargar_en_gold="INVOICE_DATE_DIM")
    

    procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
                                    tablas_en_base_de_datos_a_cargar_en_gold="COUNTRY_DIM")
    
    procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
                                    tablas_en_base_de_datos_a_cargar_en_gold="PRODUCT_DIM")
    
    procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
                                    tablas_en_base_de_datos_a_cargar_en_gold="INVOICE_FACT_TABLE")
    
    

# @task
# def task_procesar_datos_de_silver_a_gold_dim():
#     procesar_datos_de_silver_a_gold(tabla_a_obtener_en_base_de_datos_silver="INVOICE_SILVER",
#                                     tablas_en_base_de_datos_a_cargar_en_gold="INVOICE_DATE_DIM")

@task
def cargar_datos_a_base_de_datos_gold():
    cargar_datos_a_snowflake_a_gold()

@flow
def etl_prueba():
    extraccion_de_datos = extraer_datos()
    guardado_de_datos = guardar_datos()
    cargar_datos_bronze_task()
    mensaje = str(extraccion_de_datos) + " " + str(guardado_de_datos)
    print(mensaje)
    logging.info("Comienza el proceso para cargar datos a la base de datos de silver")
    obtener_datos_de_snowflake_para_el_dataset_invoice_silver()
    logging.info("Comienza el proceso para procesar datos de bronze a silver en la tabla de invoice")
    procesar_datos_de_bronze_a_silver_tabla_invoice()
    # PONER RETRY 2 PARA cargar_datos_a_snowflake_a_silver()
    logging.info("Se termino el proceso para procesar datos de bronze a silver en la tabla de invoice")
    logging.info("Se termino el proceso para cargar datos a la base de datos de silver")

    task_procesar_datos_de_silver_a_gold()


if __name__ == "__main__":
    etl_prueba.serve(name="etl_prueba",
                     tags=["etl_de_prueba", "proyecto"],
                     interval=60)