# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "845a6548-8113-48d9-b9e2-1eab240b8679",
# META       "default_lakehouse_name": "Silver_Refined",
# META       "default_lakehouse_workspace_id": "32422c8b-1ef6-4e8f-a861-012d03453d6c",
# META       "known_lakehouses": [
# META         {
# META           "id": "845a6548-8113-48d9-b9e2-1eab240b8679"
# META         },
# META         {
# META           "id": "1aa2af41-df7f-43de-8e17-183f50040fd3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Preparar data para ML - analítica predictiva

df = spark.read.table("Silver_Refined.ft_sales_dia_orden")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from Silver_Refined.ft_sales_dia_orden

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
