# Databricks notebook source
#spark.conf.set ("fs.azure.account.key.devstoragedf.blob.core.windows.net", dbutils.secrets.get(scope = "databricks-secret-scope", key = "dbstorageacckey"))
spark.conf.set("fs.azure.account.key.devstoragedf.dfs.core.windows.net", "VQQP0Z51XmZskP/uXwL9H2Ok4VA43MdIy53VrUmpCwrzqA3SWfs7WsylAeZukIl2gKmTUcwcZSGw+AStb/pKNw==")

# COMMAND ----------

import os
filepath = "wasbs://test@devstoragedf.blob.core.windows.net/inputconfigcsv" 
#outputfilepath = "wasbs://test@devstoragedf.blob.core.windows.net/output"

files = dbutils.fs.ls(filepath)
#files_list = []
 
for f in files:
    # use an if statement to determine if you want to append the path to the list
    #files_list.append(f.path)
    #display(f.path)
    filename = os.path.splitext(os.path.basename(f.path))[0]
    df = spark.read.format("csv").option("header", True).load(f.path)
    df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(filename)
    #display(df)

# COMMAND ----------

#%sql
#select * from dexportoutgoingcost

# COMMAND ----------

#%sql
#select * from etltransformlogic

# COMMAND ----------

# %sql
# select * from cloudconfig

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loadcontroltable

# COMMAND ----------

# %sql
# select * from transformlogic

# COMMAND ----------


selectQuery = "select registration_dttm as registration_dttm, id as id, first_name as first_name, last_name as last_name, concat(first_name, , last_name) as full_name, email as email, gender as gender from DMS_User where gender is not NULL and gender <>''"

# sqlTransformScript = selectQuery.replace("'", "\'")
streamLocation = 'streaming/user'
spark.sql("update loadcontroltable set SqlTransformScript = '" + selectQuery.replace("'", "''") + "' where StreamLocation = '" + streamLocation + "'")

display(selectQuery.replace("'", "''"))
# display(spark.sql("select * from loadcontroltable where StreamLocation = '" + streamLocation + "'"))