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

# Un poquito de Spark en Notebooks

# Leer la tabla del Lakehouse usando Spark
DataframeSpark = spark.read.table("Bronze_Landing.minciencia")
# Seleccionar solo las columnas deseadas en Spark
DataframeSpark = DataframeSpark.select(
    "time",
    "ff_Valor",
    "CodigoNacional"
)

# Mostrar resultado
display(DataframeSpark)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install semantic-link

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import pandas as pd

# Ruta del archivo en OneLake (Lakehouse)
path = "abfss://32422c8b-1ef6-4e8f-a861-012d03453d6c@onelake.dfs.fabric.microsoft.com/1aa2af41-df7f-43de-8e17-183f50040fd3/Tables/minciencia"

# Leer el parquet desde OneLake
df_pandas = pd.read_parquet(path)

# Filtrar solo las columnas deseadas
df_pandas = df_pandas[["time", "ff_Valor", "CodigoNacional"]]

# Mostrar resultado
df_pandas.head()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ===========================================================
# 🔥 FORECAST 2 AÑOS PARA TODAS LAS ESTACIONES (PROPHET + SPARK)
# ===========================================================

from pyspark.sql import functions as F
import pandas as pd
from prophet import Prophet

# -----------------------------------------------------------
# 1️⃣ Cargar datos y preparar columnas
# -----------------------------------------------------------

df = spark.read.table("Bronze_Landing.minciencia")

df = (
    df.select("time", "ff_Valor", "CodigoNacional")
      .withColumn("time", F.to_timestamp("time"))
)

# -----------------------------------------------------------
# 2️⃣ Calcular promedio diario por estación
# -----------------------------------------------------------

df_diario = (
    df
    .withColumn("ds", F.to_date("time"))
    .groupBy("CodigoNacional", "ds")
    .agg(F.avg("ff_Valor").alias("y"))
    .orderBy("CodigoNacional", "ds")
)

display(df_diario)

# -----------------------------------------------------------
# 3️⃣ Obtener lista de estaciones
# -----------------------------------------------------------

lista_estaciones = (
    df_diario
    .select("CodigoNacional")
    .distinct()
    .toPandas()["CodigoNacional"]
    .tolist()
)

print("Estaciones encontradas:", lista_estaciones)

# -----------------------------------------------------------
# 4️⃣ Entrenar Prophet por estación y generar forecast 2 años
# -----------------------------------------------------------

horizonte_dias = 730   # 2 años
resultados = []

for cod in lista_estaciones:

    print(f"\n========== Procesando estación {cod} ==========")

    # Filtrar datos por estación
    pdf = (
        df_diario
        .filter(F.col("CodigoNacional") == cod)
        .toPandas()
    )

    # Evitar estaciones con pocos datos
    if pdf.empty or len(pdf) < 30:
        print(f"⚠️ Saltando {cod}: muy pocos datos ({len(pdf)} registros)")
        continue

    # Preparar fecha
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    pdf = pdf.sort_values("ds")

    # -------------------------------------------------------
    # Ajustar modelo Prophet
    # -------------------------------------------------------
    modelo = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False
    )

    modelo.fit(pdf)

    # -------------------------------------------------------
    # Forecast
    # -------------------------------------------------------
    future = modelo.make_future_dataframe(periods=horizonte_dias, freq='D')
    forecast = modelo.predict(future)

    # Añadir estación al resultado
    forecast["CodigoNacional"] = cod

    # Seleccionar columnas importantes
    forecast_sel = forecast[["ds", "CodigoNacional", "yhat", "yhat_lower", "yhat_upper"]]

    resultados.append(forecast_sel)

# -----------------------------------------------------------
# 5️⃣ Unir todos los forecasts en un solo DataFrame
# -----------------------------------------------------------

if len(resultados) > 0:
    forecast_total_pd = pd.concat(resultados, ignore_index=True)
    display(forecast_total_pd)
else:
    print("❌ No se generaron predicciones.")
    raise SystemExit()

# -----------------------------------------------------------
# 6️⃣ Convertir a Spark
# -----------------------------------------------------------

forecast_total_spark = spark.createDataFrame(forecast_total_pd)

display(forecast_total_spark)

# -----------------------------------------------------------
# 7️⃣ (Opcional) Guardar en Lakehouse como tabla Delta
# -----------------------------------------------------------

# forecast_total_spark.write.format("delta").mode("overwrite") \
#     .saveAsTable("Gold_Forecast.ff_valor_forecast_2_anios")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
