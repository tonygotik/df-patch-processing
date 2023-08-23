# Databricks notebook source
# MAGIC %run ./ConnectStorageAccount

# COMMAND ----------

# MAGIC %run ./CloudFileConfiguration

# COMMAND ----------

# MAGIC %run ./TransformLogic

# COMMAND ----------

df = (spark.sql("select * from loadcontroltable"))

data_collect = df.collect()
# looping thorough each row of the dataframe
for row in data_collect:
    # while looping through each
    # row printing the data of Id, Name and City
    storageContainer = row["StorageContainer"]
    streamLocation = row["StreamLocation"]
    resultLocation = row["ResultLocation"]
    checkpointLocation = row["CheckpointLocation"]
    schemaLocation = row["SchemaLocation"]
    tableName = row["SourceTableName"]
    transformScriptLocation = row["TransformScriptLocation"]
    selectQuery = row["SqlTransformScript"]

    file_location  = storageLocationURL(storageAccount, storageContainer, streamLocation)
    checkpoint_location = storageLocationURL(storageAccount, storageContainer, checkpointLocation)
    result_location  = storageLocationURL(storageAccount, storageContainer, resultLocation)
    schema_location  = storageLocationURL(storageAccount, storageContainer, schemaLocation)
    transform_script_location  = storageLocationURL(storageAccount, storageContainer, transformScriptLocation)

    view = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .options(**AdditionalOptions)
    .load(file_location)).createOrReplaceTempView(tableName)

    # if selectQuery is not None or selectQuery == "NULL" or selectQuery == "":
    #     selectQuery = transformLogic(streamLocation, tableName)

    selectQuery = transformLogic(streamLocation, tableName)

    if selectQuery != "":
        display(selectQuery)
        (spark.sql(selectQuery)
            .writeStream.format("csv")
            .option("header", True)
            .option("checkpointLocation",checkpoint_location)
            .outputMode("Append")
            .start(result_location))
        
        spark.sql("update loadcontroltable set SqlTransformScript = '" + selectQuery + "' where StreamLocation = '" + streamLocation + "'")
        
    
    