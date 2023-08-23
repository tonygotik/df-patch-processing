# Databricks notebook source
#configure either the number of bytes or files to read as part of the config size required to infer the schema.

#spark.conf.set("spark.databricks.cloudfiles.schemaInference.sampleSize.numBytes",10000000000)
spark.conf.set("spark.databricks.cloudfiles.schemaInference.sampleSize.numFiles",10)

# COMMAND ----------

CloudFile = {
    "cloudFiles.format": "csv",
    #"cloudFiles.schemaLocation", schema_location,
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.useNotifications": "true"
}


# COMMAND ----------

AdditionalOptions = {
    #"cloudFiles.schemaHints":"customerid int",
    "rescueDataColumn":"_rescued_data"
    }