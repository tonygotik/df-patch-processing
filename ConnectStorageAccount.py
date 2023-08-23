# Databricks notebook source
df = (spark.sql("select * from cloudconfig"))
storageAccount = df.where(df["Key"] == "StorageAccount").select("Value").first()[0]
storageContainer = df.where(df["Key"] == "StorageContainer").select("Value").first()[0]
storageAccountKey = df.where(df["Key"] == "StorageAccountKey").select("Value").first()[0]

# COMMAND ----------

storageAccountURL = "fs.azure.account.key."+storageAccount+".dfs.core.windows.net"

# COMMAND ----------

spark.conf.set(storageAccountURL, storageAccountKey) 

# COMMAND ----------

def storageLocationURL(storageAccount: str, storageContainer: str, locationName: str) -> str:
    location = "abfss://"+storageContainer+"@"+storageAccount+".dfs.core.windows.net/"+locationName+"/"
    return location