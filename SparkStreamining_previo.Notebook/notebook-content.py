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

# Crear ficheros json con data aleatoria

import json
import os
import uuid
import random 
import time
from datetime import datetime


#Configuracion de los ficheros
carpeta_salida ="/lakehouse/default/Files/ficheros_jason"
num_ficheros = 15
seg_espera= 7

#verificacion de existencia de carpetas
os.makedirs(carpeta_salida, exist_ok=True)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_ficheros):

    #variables generales
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%dT%H%M%S%f")

    #simulacion de mensaje del sensor
    registro = {
        "id": str(uuid.uuid4()),
        "temp": round(random.uniform(21.0, 35.0), 2),
        "timestamp": timestamp_str
    }

    #Construir el fichero json
    filename= f"temp_{timestamp_str}.json"
    filepath = os.path.join(carpeta_salida,filename)

    #materializar el fichero JSON 
    with open (filepath, "w") as f:
        json.dump(registro,f)
    

    print (f"OK- [{i+1}/{num_ficheros}] Escribio: {filename}")
    time.sleep(seg_espera)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
