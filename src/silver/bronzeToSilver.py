# Databricks notebook source
# try:
#   dbutils.fs.mount(
#     source = "wasbs://bronze@arjung2.blob.core.windows.net/Bronze/sales_view",
#     mount_point = "/mnt/aa",
#     extra_configs = {####################3})
# except Exception as e:
#   print('mount already existed')

# COMMAND ----------

# try:
#   dbutils.fs.mount(
#     source = "wasbs://bronze@arjung2.blob.core.windows.net",
#     mount_point = "/mnt/cp",
#     extra_configs = {##############})
# except Exception as e:
#   print('mount already existed')

# COMMAND ----------

file_name = '/mnt/aa/customers'

# COMMAND ----------

customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name)


# COMMAND ----------

def convertColumnName(i):
    sc = ''
    for c in i:
        if c.isupper():
            sc += c.lower()
        elif c==' ':
            sc += '_'
        else:
            sc += c
    return sc

# COMMAND ----------

from pyspark.sql.functions import *
sc = udf(convertColumnName)
customer_df = customer_df.select([customer_df[col].alias(convertColumnName(col)) for col in customer_df.columns])

# COMMAND ----------

customer_df = customer_df.withColumn('first_name', split(col('Name'), ' ')[0]).withColumn('last_name', split(col('Name'), ' ')[1])

# COMMAND ----------

customer_df = customer_df.withColumn('domain', split(col('email_id'), '@')[0])


# COMMAND ----------

customer_df = customer_df.withColumn('gender', when(col('gender')=='male', 'M').when(col('gender')=='female', 'F'))


# COMMAND ----------

customer_df = customer_df.withColumn('date', split(col('joining_date'), ' ')[0]).withColumn('time', split(col('joining_date'), ' ')[1])


# COMMAND ----------

change_datetype = customer_df.withColumn("date",to_date("date","dd-MM-yyyy"))


# COMMAND ----------

customer_df = change_datetype.withColumn("date", date_format("date","yyyy-MM-dd"))


# COMMAND ----------

customer_df = customer_df.withColumn('expenditure-status', when(col('spent')<200, 'MINIMUM').otherwise('MAXIMUM'))


# COMMAND ----------

customer_df.write.format("delta").mode("overwrite").save("/mnt/cp/silver/sales_view/customers")

# COMMAND ----------

file_name1 = '/mnt/aa/products'
product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name1)


# COMMAND ----------

product_df = product_df.select([product_df[col].alias(convertColumnName(col)) for col in product_df.columns])


# COMMAND ----------

product_df = product_df.withColumn('sub_category', when(product_df.category_id==1,'phone').when(product_df.category_id==2,'laptop').when(product_df.category_id==3,'playstation').when(product_df.category_id==4,'e-device'))


# COMMAND ----------

product_df.write.format("delta").mode("overwrite").save("/mnt/cp/silver/sales_view/products")

# COMMAND ----------

file_name2 = '/mnt/aa/stores'
store_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name2)


# COMMAND ----------

store_df = store_df.select([store_df[col].alias(convertColumnName(col)) for col in store_df.columns])


# COMMAND ----------



# COMMAND ----------

store_df = store_df.withColumn('store_category', split(split(store_df.email_address,'@')[1], '\.')[0])


# COMMAND ----------

store_df.write.format("delta").mode("overwrite").save("/mnt/cp/silver/sales_view/stores")

# COMMAND ----------

file_name3 = '/mnt/aa/sales'
sales_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name3)
sales_df = sales_df.select([sales_df[col].alias(convertColumnName(col)) for col in sales_df.columns])
sales_df.write.format("delta").mode("overwrite").save("/mnt/cp/silver/sales_view/sales")


# COMMAND ----------

