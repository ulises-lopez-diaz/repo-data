import os
from dotenv import load_dotenv
import snowflake.connector
from pyspark.sql import SparkSession

load_dotenv()




# Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sfOptionsGoldInvoice = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_GOLD"),
    "sfSchema": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"),
    "sfWarehouse": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
    "sfRole": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE")
}