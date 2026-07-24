# Databricks notebook source
# MAGIC %md
# MAGIC #Create Catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog car_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC #Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema car_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema car_catalog.gold;