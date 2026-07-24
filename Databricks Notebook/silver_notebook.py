# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading

# COMMAND ----------

df=spark.read.format('parquet')\
        .option('inferschema',True)\
        .load('abfss://bronze@carsdatalakemonk.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df= df.withColumn('Model_catagory',split(col("Model_ID"),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col("Units_Sold").cast(StringType())).display()

# COMMAND ----------

df= df.withColumn('RevperUnits',col("Revenue")/col("Units_Sold"))
df.display()

# COMMAND ----------

df.groupBy("Year",'BranchName').agg(sum("Units_Sold")).display()

# COMMAND ----------

df.groupBy("Year",'BranchName').agg(sum("Units_Sold").alias("Total_Units")).sort("Year","Total_Units",ascending=[True,False]).display()

# COMMAND ----------

display(df.groupBy("Year",'BranchName').agg(sum("Units_Sold").alias("Total_Units")).sort("Year","Total_Units",ascending=[True,False]))

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Writing

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales")\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales`