from pyspark.sql.functions import col, round, sum as _sum, avg, count, countDistinct

print("Starting Lakehouse Analytics & Optimization Routine...")

# ------------------------------------------------------------------------------
# 1. Delta Lake Optimization & Compaction (Z-Ordering)
# ------------------------------------------------------------------------------
# Compact small files and co-locate data by high-cardinality join/filter keys
print("\n--- 1. Running Z-Order Optimization ---")
spark.sql("""
    OPTIMIZE gold_fact_order_sales 
    ZORDER BY (order_date, customer_id, product_id)
""")

spark.sql("""
    OPTIMIZE gold_dim_customer 
    ZORDER BY (customer_id, customer_state)
""")

# ------------------------------------------------------------------------------
# 2. Table Maintenance & Garbage Collection (VACUUM)
# ------------------------------------------------------------------------------
# Remove uncommitted files or data older than the retention threshold
print("\n--- 2. Executing Delta Table VACUUM (Dry Run / Retention) ---")
# spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
spark.sql("VACUUM gold_fact_order_sales RETAIN 168 HOURS")  # Retain 7 days of historical logs

# ------------------------------------------------------------------------------
# 3. Delta Table History & Time Travel Verification
# ------------------------------------------------------------------------------
print("\n--- 3. Inspecting Delta Transaction History ---")
history_df = spark.sql("DESCRIBE HISTORY gold_fact_order_sales")
display(history_df.select("version", "timestamp", "userId", "operation", "operationMetrics"))

# Example: Querying previous version (Time Travel demonstration)
# df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("gold_fact_order_sales")
# print(f"Row count at Version 0: {df_v0.count()}")

# ------------------------------------------------------------------------------
# 4. Business KPI Aggregations (Serving Power BI / Reporting Layer)
# ------------------------------------------------------------------------------
print("\n--- 4. Computing Core Business Analytics ---")

# Load Gold dimension and fact tables
fact_sales = spark.read.table("gold_fact_order_sales")
dim_cust = spark.read.table("gold_dim_customer")
dim_prod = spark.read.table("gold_dim_product")
dim_seller = spark.read.table("gold_dim_seller")

# KPI 1: Regional Sales & Average Order Value (AOV) by Customer State
regional_sales = fact_sales.join(dim_cust, "customer_id", "inner") \
    .groupBy("customer_state") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(_sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_order_value"),
        round(_sum("freight_value"), 2).alias("total_freight_cost")
    ) \
    .orderBy(col("total_revenue").desc())

print("\n--- Top Performing States by Revenue ---")
display(regional_sales)

# KPI 2: Top Product Categories by Revenue and Freight Ratio
category_performance = fact_sales.join(dim_prod, "product_id", "inner") \
    .groupBy("product_category_name") \
    .agg(
        countDistinct("order_id").alias("units_sold"),
        round(_sum("price"), 2).alias("total_category_revenue"),
        round(avg("freight_value"), 2).alias("avg_freight_per_item")
    ) \
    .filter(col("product_category_name").isNotNull()) \
    .orderBy(col("total_category_revenue").desc()) \
    .limit(10)

print("\n--- Top 10 Product Categories ---")
display(category_performance)

# KPI 3: Seller Fulfillment Volume & Order Status Distribution
order_status_dist = fact_sales.groupBy("order_status") \
    .agg(
        count("order_id").alias("order_count"),
        round(_sum("price"), 2).alias("revenue_impact")
    ) \
    .orderBy(col("order_count").desc())

print("\n--- Order Fulfillment Status Breakdown ---")
display(order_status_dist)

print("\nLakehouse Analytics execution complete.")