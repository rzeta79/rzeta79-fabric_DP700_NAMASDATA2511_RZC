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
# META         },
# META         {
# META           "id": "845a6548-8113-48d9-b9e2-1eab240b8679"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

## Trabajamos las tablas sin timepo complejo (materializar parquets en Tablas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer los ficheros parquet que son el resultado de la importacion anterior

df_department= spark.read.parquet("Files/humanresources/department")
df_employeedh = spark.read.parquet("Files/humanresources/employeedepartmenthistory")
df_employeeph = spark.read.parquet("Files/humanresources/employeepayhistory")
df_jobcandidate = spark.read.parquet("Files/humanresources/jobcandidate")
# df_shift
display(df_department)
display(df_employeedh)
display(df_employeeph)
display(df_jobcandidate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardar los dataframes como tablas

df_department.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_department")
df_employeedh.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_employeedepartmenthistory")
df_employeeph.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Trabajamos la tabla mas compleja

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

#lee y limpia
df_shift_pandas =pd.read_parquet("/lakehouse/default/Files/humanresources/shift")
df_shift_pandas= df_shift_pandas.drop(columns=["modifieddate"])
display(df_shift_pandas)

# 2) Normaliza tipos : time -> string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)   # "HH:MM:SS"

display(df_shift_pandas)

# 3) De pandas a Spark
df_shift = spark.createDataFrame(df_shift_pandas)

# 4) Casts finales
df_shift = (
    df_shift
        .withColumn("shiftid", F.col("shiftid").cast("int"))
        .withColumn("name",    F.col("name").cast("string"))
        .withColumn("starttime", F.col("starttime").cast("string"))
        .withColumn("endtime",   F.col("endtime").cast("string"))
)

# 5) Guarda la tabla Delta en Bronze
df_shift.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Bronze_Landing.hr_shift")
# Verificación
spark.sql("SELECT * FROM Bronze_landing.hr_shift").show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
