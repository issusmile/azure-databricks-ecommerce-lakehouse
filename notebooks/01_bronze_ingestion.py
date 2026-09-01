from pyspark.sql.functions import current_timestamp, input_file_name, col

# #Define DBFS input paths
# orders_raw_path = "/Volumes/brazilian_ecommerce/ecommerce/raw_data/olist_orders_dataset.csv"
# items_raw_path = "/Volumes/brazilian_ecommerce/ecommerce/raw_data/olist_order_items_dataset.csv"

# # 1. Read raw csv
# df_orders_raw = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(orders_raw_path)

# # df_orders_raw.printSchema()
# # df_orders_raw.show(5)

# # 2. Append ingestion audit columns
# df_orders_bronze = df_orders_raw \
#     .withColumn("_ingestion_timestamp", current_timestamp()) \
#     .withColumn("_source_file", col("_metadata.file_path"))

# # display(df_orders_bronze)
# # display(df_orders_bronze.limit(20))

# # 3. Write to  Bronze Delta  Table
# df_orders_bronze.write.format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("bronze_orders")

# print("Bronze tables created successfully.")


# Metadata driven dynamic ingestion loop

#  Deine all datasets to ingest: (source_filename, table_name)
datasets_to_ingest = [
    ("olist_orders_dataset.csv", "bronze_orders"),
    ("olist_order_items_dataset.csv", "bronze_order_items"),
    ("olist_order_payments_dataset.csv", "bronze_order_payments"),
    ("olist_customers_dataset.csv","bronze_customers"),
    ("olist_order_reviews_dataset.csv","bronze_order_reviews"), #olist_order_reviews_dataset.csv
    ("olist_products_dataset.csv","bronze_products"),
    ("olist_sellers_dataset.csv","bronze_sellers"),
    ("product_category_name_translation.csv","bronze_product_category_translation"),
    ("olist_geolocation_dataset.csv","bronze_geolocation")
]

base_path = "/Volumes/brazilian_ecommerce/ecommerce/raw_data/"

# Iterete dynamically though all datasets
for file_name, table_name in datasets_to_ingest:
    print(f"Ingesting {file_name} into {table_name}...")

    file_path = f"{base_path}{file_name}"

    # Read raw csv
    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    # Append ingestion audit columns
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

    # Write to Bronze Delta Table
    df_bronze.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    print(f"Successfully created Delta table:{table_name}(Count:{df_bronze.count()})")
print("All Bronze tables succesfully loaded.")

# files = dbutils.fs.ls("dbfs:/Volumes/brazilian_ecommerce/ecommerce/raw_data/")
# files_df = spark.createDataFrame(files)
# display(files_df.select("name", "path"))

# display(dbutils.fs.ls("dbfs:/Volumes/brazilian_ecommerce/ecommerce/raw_data/"))

# for f in dbutils.fs.ls("dbfs:/Volumes/brazilian_ecommerce/ecommerce/raw_data/"):
#     print(f.name)
# olist_customers_dataset.csv
# olist_geolocation_dataset.csv
# olist_order_items_dataset.csv
# olist_order_payments_dataset.csv
# olist_order_reviews_dataset.csv
# olist_orders_dataset.csv
# olist_products_dataset.csv
# olist_sellers_dataset.csv
# product_category_name_translation.csv



              

