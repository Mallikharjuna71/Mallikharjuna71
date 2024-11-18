# Databricks notebook source
# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/bronzeToSilver

# COMMAND ----------

store_df.columns

# COMMAND ----------

product_df.columns

# COMMAND ----------

product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner").select(store_df.store_id, store_df.store_name, store_df.location, store_df.manager_name,product_df.product_id, product_df.product_name, product_df.product_code, product_df.description,product_df.category_id, product_df.price, product_df.stock_quantity, product_df.supplier_id, product_df.created_at, product_df.updated_at, product_df.image_url, product_df.weight,product_df.expiry_date, product_df.is_active, product_df.tax_rate)


# COMMAND ----------

sales_df.columns


# COMMAND ----------

final_df = product_store_df.join(sales_df, product_store_df.product_id == sales_df.product_id, "inner").select(sales_df.orderdate, sales_df.category, sales_df.city, sales_df.customerid, sales_df.orderid, sales_df.product_id, sales_df.profit, sales_df.region, sales_df.sales, sales_df.segment, sales_df.shipdate, sales_df.shipmode, sales_df.latitude, sales_df.longitude, product_store_df.store_name, product_store_df.location, product_store_df.manager_name, product_store_df.product_name, product_store_df.price, product_store_df.stock_quantity, product_store_df.image_url)

# COMMAND ----------

final_df.display()

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save("/mnt/cp/gold/sales_view/StoreProductSalesAnalysis")
