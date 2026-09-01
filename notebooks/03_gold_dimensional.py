from pyspark.sql.functions import monotonically_increasing_id, col, year, month, dayofmonth, date_format, to_date

# --- Dimension 1: Products ---
silver_products = spark.read.table("silver_products")
dim_product = silver_products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).withColumn("product_dim_key", monotonically_increasing_id())

dim_product.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")

# --- Dimension 2: Sellers ---
silver_sellers = spark.read.table("silver_sellers")
dim_seller = silver_sellers.select(
    "seller_id",
    "seller_city",
    "seller_state"
).withColumn("seller_dim_key", monotonically_increasing_id())

dim_seller.write.format("delta").mode("overwrite").saveAsTable("gold_dim_seller")

# --- Dimension 3: Customers ---
silver_customers = spark.read.table("silver_customers")
dim_customer = silver_customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
).withColumn("customer_dim_key", monotonically_increasing_id())

dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold_dim_customer")

# --- Dimension 4: Date Dimension ---
silver_orders = spark.read.table("silver_orders")
dim_date = silver_orders.select(to_date("order_purchase_timestamp").alias("order_date")) \
    .distinct() \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("day", dayofmonth("order_date")) \
    .withColumn("month_name", date_format("order_date", "MMMM")) \
    .withColumn("day_of_week", date_format("order_date", "EEEE")) \
    .withColumn("date_dim_key", monotonically_increasing_id())

dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")

# --- Central Fact Table: Order Sales & Fulfillment ---
silver_items = spark.read.table("silver_order_items")
silver_payments = spark.read.table("silver_order_payments")

# Aggregate payments to order-level to avoid duplicate grain explosion
order_payments_agg = silver_payments.groupBy("order_id").sum("payment_value") \
    .withColumnRenamed("sum(payment_value)", "total_payment_value")

# Join Order headers, Order items, and Payments
fact_order_sales = silver_orders.join(silver_items, "order_id", "inner") \
    .join(order_payments_agg, "order_id", "left") \
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("seller_id"),
        to_date(col("order_purchase_timestamp")).alias("order_date"),
        col("order_status"),
        col("price"),
        col("freight_value"),
        col("total_payment_value")
    )

fact_order_sales.write.format("delta").mode("overwrite").saveAsTable("gold_fact_order_sales")

print("Complete Gold Star Schema deployed successfully.")