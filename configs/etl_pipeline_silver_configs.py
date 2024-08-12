import os
from dotenv import load_dotenv
import snowflake.connector


load_dotenv()

# Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


sfOptionsSilver = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_SILVER"),
    "sfSchema": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"),
    "sfWarehouse": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
    "sfRole": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE")
}

snowflake_invoice_silver_connection = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
        database=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE_SILVER"),
        schema=os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA")
    )
