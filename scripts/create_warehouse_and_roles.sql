create warehouse if not exists retail_wh with warehouse_size='x-small';
create database if not exists retail_db_bronze;
create database if not exists retail_db_silver;
create database if not exists retail_db_gold;
create role if not exists retail_warehouse_role;

grant usage on warehouse retail_wh to role retail_warehouse_role;
grant role retail_warehouse_role to user ULISESLOPEZDIAZDATA3;
grant all on database retail_db_bronze to role retail_warehouse_role;

use role retail_warehouse_role;
