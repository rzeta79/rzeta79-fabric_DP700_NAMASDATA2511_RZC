# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1aa2af41-df7f-43de-8e17-183f50040fd3",
# META       "default_lakehouse_name": "Bronze_Landing",
# META       "default_lakehouse_workspace_id": "32422c8b-1ef6-4e8f-a861-012d03453d6c",
# META       "known_lakehouses": [
# META         {
# META           "id": "1aa2af41-df7f-43de-8e17-183f50040fd3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Note de Ingesta 

#Incovar librerias
from pyspark.sql.types import StructType,StringType,DoubleType,TimestampType
import os
import json
import pyspark.sql.functions as F
import time

#configuracion
ruta_origen ="Files/ficheros_jason"
nombre_tabla = "temperatura_simulada"
ruta_check ="Files/ficheros_checkpoint"

#Esquema del fichero de origen (data json de origen)
file_schema = StructType() \
    .add("id", StringType()) \
    .add("temp",DoubleType()) \
    .add("timestamp", TimestampType())

spark.sql(f"Create table if not exists {nombre_tabla}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer el fichero de origen con Spark
fichero_raiz_df = spark.readStream \
    .schema(file_schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(ruta_origen)

# Agregar el timestamp de procesamiento
fichero_mod_df = fichero_raiz_df \
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyyMMdd'T'HHmmss")
    ) \
    .withColumn(
        "ts_proces",
        F.current_timestamp()
    )

#escribimos la data en la tabla delta
deltastream = fichero_mod_df\
    .writeStream\
    .format("delta") \
    .outputMode("append") \
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_check)\
    .start(f"Tables/{nombre_tabla}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("Bronze_Landing.temperatura_simulada")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fichero_raiz_df.isStreaming

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.isActive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.lastProgress

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

while deltastream.isActive:
    print("✅ Stream is running...")
    print("📊 Last progress:", deltastream.lastProgress)
    time.sleep(5)

print("❌ Stream has stopped.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

dfst =spark.sql("SELECT COUNT(*) FROM Bronze_Landing.temperatura_simulada")
display(dfst)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
