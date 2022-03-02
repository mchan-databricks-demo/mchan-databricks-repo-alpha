# Databricks notebook source
# MAGIC %md
# MAGIC # 00:CONFIG:DBFS_COMMANDS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. List all Mounts
# MAGIC * Pointers to underlying ADLS 

# COMMAND ----------

# MAGIC %python 
# MAGIC mountList = dbutils.fs.mounts() 
# MAGIC for mnt in mountList:
# MAGIC     print(mnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. List all files in a particular bucket

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/mchan-retail/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Copy files from one path to another

# COMMAND ----------

dbutils.fs.cp("dbfs:/first_path/", "dbfs:/second_path/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Read and Write Files

# COMMAND ----------

dbutils.fs.mkdirs(“dbfs:/Mendelsohn.chan/")
dbutils.fs.put("/foobar/baz.txt", "Hello, World!")
dbutils.fs.head("/foobar/baz.txt")
dbutils.fs.rm("/foobar/baz.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Preview the file 

# COMMAND ----------

dbutils.fs.head("/mnt/mchan-retail/RAW_ORDERS_2020.csv/", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Example for string formatting

# COMMAND ----------

# Default location is: dbfs:/user/hive/warehouse/mchan_demo_database.db
databaseName = "mchan_demo_database_alpha"

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {databaseName}
""")
spark.sql(f"USE {databaseName}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Run a config notebook 

# COMMAND ----------

dbutils.notebook.run("/Users/mendelsohn.chan@databricks.com/00_NOTEBOOKS_REF/00:CONFIG_DBFS_COMMANDS"

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 8. Mount ADLS on DBFS

# COMMAND ----------

# PROD WORKSPACE start with 6
# Go to secret-scope URL first below
https://adb-6464564345757588.8.azuredatabricks.net/?o=6464564345757588#secrets/createScope

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://mchan-test@mchanstorage2.blob.core.windows.net",
    mount_point = "/mnt/mchan-test", 
    extra_configs = {"fs.azure.account.key.mchanstorage2.blob.core.windows.net":dbutils.secrets.get(scope = "databricks-  mchanstorage2-`secretscope", key = "mchanstorage2key")}
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 9. List all the secret scopes

# COMMAND ----------

# List all the scopes found in Databricks Secret Scopes 
for x in dbutils.secrets.listScopes():
    print(x)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 10. Access all the secrets stored in a secret scope 
# MAGIC * A secret is a key-value pair like (Birthday: Dec 24, 1991)

# COMMAND ----------

dbutils.secrets.list("mchanstorage2-secretscope")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Access the secret value in a particular secret scope

# COMMAND ----------

dbutils.secrets.get(scope = "mchanstorage2-secretscope", key = "mchanbirthday")

# COMMAND ----------

for x in dbutils.secrets.get(scope = "mchanstorage2-secretscope", key = "mchanbirthday"):
    print(x) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Download files in FileStore to Local Machine
# MAGIC ### 
# MAGIC - This is the actual path of the file: ```/dbfs/FileStore/tables/FACT_ORDERS.csv```
# MAGIC - ```https://<databricks-instance>/?o=6280049833xxxxxx```, replace with 
# MAGIC - ```https://adb-6464564345757588.8.azuredatabricks.net/files/tables/FACT_ORDERS.csv```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Azure Databricks Create Secret Scope UI 

# COMMAND ----------

# https://<databricks-instance>#secrets/createScope
# https://adb-6464564345757588.8.azuredatabricks.net/#secrets/createScope

# COMMAND ----------

# MAGIC %md
# MAGIC # -- END -- 
