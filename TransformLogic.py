# Databricks notebook source
def transformLogic(source_location:str, table_name: str) -> str: 

    df = spark.sql("select * from transformlogic where SourceLocation = '" + source_location + "'")
    
    if df.rdd.isEmpty():
        return ""
    else:
        data_collect = df.collect()
        selectQuery = "select "
        fromQuery = " from " + table_name
        filterCondition = str("")
        groupBy = str("")
        fullQuery = ""
        # looping thorough each row of the dataframe
        for row in data_collect:
            if selectQuery != "select ":
                selectQuery += ", "
            if row["Fomular"] is not None and row["Fomular"] != "NULL" and row["Fomular"] != "":
                selectQuery += row["Fomular"] + " as " + row["SinkColumnName"]
            else:
                selectQuery += row["SourceColumnName"] + " as " + row["SinkColumnName"]

            if row["Filter"] is not None and row["Filter"] != "NULL" and row["Filter"] != "":
                if filterCondition == "":
                    filterCondition = " where " + row["Filter"] 
                else:
                    filterCondition += " and " + row["Filter"]      
            else:
                filterCondition = filterCondition
            
            if row["IsGroupBy"] is not None and row["IsGroupBy"] != "0":
                if groupBy == "":
                    groupBy = " group by " + row["SourceColumnName"]
                else:
                    groupBy += ", " + row["SourceColumnName"]

        fullQuery = selectQuery + fromQuery + filterCondition + groupBy

        return fullQuery
        #display(fullQuery)
        #display(filterCondition)


        

# COMMAND ----------

transformLogic("streaming/user", "person")

# COMMAND ----------

# %sql
# select * from transformlogic