from  pyspark.sql.functions import col, to_timestamp


# # Read from Bronze Delta table
# df_bronze_orders = spark.read.table("bronze_orders")

# # Cast datetime fields and clean records
# df_silver_orders = df_bronze_orders \
#     .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
#     .withColumn("order_approved_at", to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss")) \
#     .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")) \
#     .filter(col("order_id").isNotNull()) \
#     .dropDuplicates(["order_id"])
                     
        
# # display(df_silver_orders.limit(10))
# # df_silver_orders.count()
# # df_bronze_orders.count()
# print(f"Silver count: {df_silver_orders.count()}")
# print(f"Bronze count: {df_bronze_orders.count()}")



# --- 1. Silver Orders ---
df_silver_orders = spark.read.table("bronze_orders") \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"))) \
    .filter(col("order_id").isNotNull()) \
    .dropDuplicates(["order_id"])

df_silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# --- 2. Silver Order Items ---
df_silver_items = spark.read.table("bronze_order_items") \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .filter(col("order_id").isNotNull() & col("product_id").isNotNull()) \
    .dropDuplicates(["order_id", "order_item_id"])

df_silver_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")

# --- 3. Silver Order Payments ---
df_silver_payments = spark.read.table("bronze_order_payments") \
    .withColumn("payment_installments", col("payment_installments").cast("int")) \
    .withColumn("payment_value", col("payment_value").cast("double")) \
    .filter(col("order_id").isNotNull())

df_silver_payments.write.format("delta").mode("overwrite").saveAsTable("silver_order_payments")

# --- 4. Silver Customers ---
df_silver_customers = spark.read.table("bronze_customers") \
    .filter(col("customer_id").isNotNull()) \
    .dropDuplicates(["customer_id"])

df_silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")

# --- 5. Silver Products ---
df_silver_products = spark.read.table("bronze_products") \
    .filter(col("product_id").isNotNull()) \
    .fillna({"product_category_name": "unknown"}) \
    .dropDuplicates(["product_id"])

df_silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")

# --- 6. Silver Sellers ---
df_silver_sellers = spark.read.table("bronze_sellers") \
    .filter(col("seller_id").isNotNull()) \
    .dropDuplicates(["seller_id"])

df_silver_sellers.write.format("delta").mode("overwrite").saveAsTable("silver_sellers")

print("All Silver tables cleaned, typed, and saved.")
