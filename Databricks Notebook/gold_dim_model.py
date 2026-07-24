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
                 select distinct(Model_ID)as Model_ID, Model_catagory 
                 from parquet.`abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales`
                 ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model sink:- initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('car_catalog.gold.dim_model'):
    df_sink=spark.sql('''
                select dim_model_key,Model_ID, Model_catagory 
                from car_catalog.gold.dim_model
                ''')
else:
    df_sink=spark.sql('''
                select 1 as dim_model_key,Model_ID, Model_catagory 
                from parquet.`abfss://silver@carsdatalakemonk.dfs.core.windows.net/carsales`
                where 1=0
                ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### filtering old and new records

# COMMAND ----------

df_flt=df_src.join(df_sink,df_src['Model_ID']==df_sink['Model_ID'],'left')\
    .select(df_src['Model_ID'],df_src['Model_catagory'],df_sink['dim_model_key'])

# COMMAND ----------



# COMMAND ----------

df_flt_old=df_flt.filter(col("dim_model_key").isNotNull())

# COMMAND ----------

df_flt_new = df_flt.filter(col("dim_model_key").isNull()).select(df_src['Model_ID'],df_src['Model_catagory'])

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
    max_df=spark.sql('select max(dim_model_key) from car_catalog.gold.dim_model')
    max_value=max_df.collect()[0][0]+1

# COMMAND ----------

df_flt_new = df_flt_new.withColumn("dim_model_key",max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists("car_catalog.gold.dim_model"):
    delta_tbl=DeltaTable.forPath(spark,'abfss://gold@carsdatalakemonk.dfs.core.windows.net/dim_model')
    delta_tbl.alias("tgr").merge(df_final.alias("src"),'tgr.dim_model_key=src.dim_model_key')\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
#initial run
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@carsdatalakemonk.dfs.core.windows.net/dim_model')\
        .saveAsTable('car_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.dim_model