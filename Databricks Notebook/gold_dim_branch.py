# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC #create flag parameter

# COMMAND ----------

dbutils.widgets.text("incremental_flag",'0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get("incremental_flag")

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating Dimension Model

# COMMAND ----------

# MAGIC %md
# MAGIC ##fatch relative column

# COMMAND ----------

df_src=spark.sql('''
                 select distinct(Branch_ID)as Branch_ID, BranchName 
                 from parquet.`abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales`
                 ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_branch sink:- initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('car_catalog.gold.dim_branch'):
    df_sink=spark.sql('''
                select dim_branch_key,Branch_ID, BranchName
                from car_catalog.gold.dim_branch
                ''')
else:
    df_sink=spark.sql('''
                select 1 as dim_branch_key,Branch_ID, BranchName 
                from parquet.`abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales`
                where 1=0
                ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### filtering old and new records

# COMMAND ----------

df_flt=df_src.join(df_sink,df_src['Branch_ID']==df_sink['Branch_ID'],'left')\
    .select(df_src['Branch_ID'],df_src['BranchName'],df_sink['dim_branch_key'])

# COMMAND ----------

df_flt.display()

# COMMAND ----------

df_flt_old=df_flt.filter(col("dim_branch_key").isNotNull())

# COMMAND ----------

df_flt_old.display()

# COMMAND ----------

df_flt_new = df_flt.filter(col("dim_branch_key").isNull()).select(df_src['Branch_ID'],df_src['BranchName'])

# COMMAND ----------

df_flt_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fatch max surrogate key from exiting table**

# COMMAND ----------

if incremental_flag=='0':
    max_value=1
else:
    max_df=spark.sql('select max(dim_branch_key) from car_catalog.gold.dim_branch')
    max_value=max_df.collect()[0][0]+1

# COMMAND ----------

df_flt_new = df_flt_new.withColumn("dim_branch_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_flt_new.display()

# COMMAND ----------

df_final=df_flt_new.union(df_flt_old)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD Type1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incrementa run
if spark.catalog.tableExists("car_catalog.gold.dim_branch"):
    delta_tbl=DeltaTable.forPath(spark,'abfss://gold@carsdatalakemonk.dfs.core.windows.net/dim_branch')
    delta_tbl.alias("tgr").merge(df_final.alias("src"),'tgr.dim_branch_key=src.dim_branch_key')\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
#initial run
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@carsdatalakemonk.dfs.core.windows.net/dim_branch')\
        .saveAsTable('car_catalog.gold.dim_branch')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.dim_branch