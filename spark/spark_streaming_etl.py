from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import yaml
from pathlib import Path

# Load configuration
def load_config():
    config_path = Path(__file__).parent / 'config/spark_config.yml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# Define schemas with record_type
schemas = {
    "orders": StructType()
        .add("OrderID", StringType())
        .add("CustomerID", StringType())
        .add("ProductID", StringType())
        .add("OrderDate", StringType())
        .add("TotalAmount", DoubleType())
        .add("PaymentMethod", StringType())
        .add("PaymentStatus", StringType())
        .add("DeliveryDate", StringType())
        .add("PromoID", StringType())
        .add("DateID", StringType())
        .add("DiscountApplied", DoubleType())
        .add("OrderStatus", StringType())
        .add("record_type", StringType()),

    "customers": StructType()
        .add("CustomerID", StringType())
        .add("Name", StringType())
        .add("Email", StringType())
        .add("Address", StringType())
        .add("SignupDate", StringType())
        .add("record_type", StringType()),

    "products": StructType()
        .add("ProductID", StringType())
        .add("Name", StringType())
        .add("Category", StringType())
        .add("Price", DoubleType())
        .add("SupplierID", StringType())
        .add("record_type", StringType()),

    "suppliers": StructType()
        .add("SupplierID", StringType())
        .add("Name", StringType())
        .add("ContactInfo", StringType())
        .add("record_type", StringType()),

    "promotions": StructType()
        .add("PromoID", StringType())
        .add("Discount", DoubleType())
        .add("StartDate", StringType())
        .add("EndDate", StringType())
        .add("CampaignType", StringType())
        .add("record_type", StringType()),

    "inventory": StructType()
        .add("InventoryID", StringType())
        .add("ProductID", StringType())
        .add("SupplierID", StringType())
        .add("StockLevel", IntegerType())
        .add("LastUpdated", StringType())
        .add("record_type", StringType()),

    "time": StructType()
        .add("DateID", StringType())
        .add("Date", StringType())
        .add("Week", IntegerType())
        .add("Month", IntegerType())
        .add("Quarter", IntegerType())
        .add("Year", IntegerType())
        .add("record_type", StringType())
}

# Spark session
spark = SparkSession.builder \
    .appName("KinesisSparkStreamingETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kinesis stream
raw_stream_df = spark.readStream \
    .format("aws-kinesis") \
    .option("kinesis.streamName", config['kinesis_stream']) \
    .option("kinesis.startingPosition", "TRIM_HORIZON") \
    .option("kinesis.awsAccessKeyId", config['aws_access_key_id']) \
    .option("kinesis.awsSecretKey", config['aws_secret_access_key']) \
    .option("kinesis.endpointUrl", "https://kinesis.us-east-2.amazonaws.com") \
    .option("kinesis.maxFetchTimeInMs", "2000") \
    .option("kinesis.maxFetchRecordsPerShard", "100") \
    .option("kinesis.maxRetryIntervalMs", "10000") \
    .load()

# .option("kinesis.startingPosition", "TRIM_HORIZON") \

json_df = raw_stream_df.selectExpr("CAST(data AS STRING) as data")

# Write function
def write_to_redshift(df, batch_id, table_name):
    print(f"Writing batch {batch_id} to {table_name}...")
    df.write \
        .format("jdbc") \
        .option("url", config['redshift_url']) \
        .option("dbtable", table_name) \
        .option("user", config['redshift_user']) \
        .option("password", config['redshift_password']) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("append") \
        .save()

# Register streaming handlers
def handle_entity(entity):
    entity_stream = json_df \
        .select(from_json(col("data"), schemas[entity]).alias("record")) \
        .filter(col("record.record_type") == entity) \
        .select("record.*")

    def batch_writer(df, batch_id):
        try:
            if df.isEmpty():
                print(f"{entity} batch {batch_id} is empty. Skipping.")
                return
            df.cache()
            print(f"Processing {entity} batch {batch_id} with {df.count()} rows")

            if entity == "orders":
                df = df.select(
                    col("OrderID").cast("int"),
                    col("CustomerID").cast("int"),
                    col("ProductID").cast("int"),
                    col("PromoID").cast("int"),
                    col("DateID").cast("int"),
                    col("TotalAmount"),
                    col("DiscountApplied"),
                    col("PaymentMethod"),
                    col("PaymentStatus"),
                    to_date("DeliveryDate").alias("DeliveryDate"),
                    col("OrderStatus")
                )
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['fact_orders_table'])

            elif entity == "inventory":
                df = df.select(
                    col("InventoryID").cast("int"),
                    col("ProductID").cast("int"),
                    col("SupplierID").cast("int"),
                    col("StockLevel"),
                    to_timestamp("LastUpdated").alias("LastUpdated")
                )
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['fact_inventory_table'])

            elif entity == "customers":
                df = df.select(
                    col("CustomerID").cast("int"),
                    col("Name"),
                    col("Email"),
                    col("Address"),
                    to_date("SignupDate").alias("SignupDate")
                ).dropDuplicates(["CustomerID"])
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['dim_customers_table'])

            elif entity == "products":
                df = df.select(
                    col("ProductID").cast("int"),
                    col("Name"),
                    col("Category"),
                    col("Price")
                ).dropDuplicates(["ProductID"])
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['dim_products_table'])

            elif entity == "suppliers":
                df = df.select(
                    col("SupplierID").cast("int"),
                    col("Name"),
                    col("ContactInfo")
                ).dropDuplicates(["SupplierID"])
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['dim_suppliers_table'])

            elif entity == "promotions":
                df = df.select(
                    col("PromoID").cast("int"),
                    col("Discount"),
                    to_date("StartDate").alias("StartDate"),
                    to_date("EndDate").alias("EndDate"),
                    col("CampaignType")
                ).dropDuplicates(["PromoID"])
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['dim_promotions_table'])

            elif entity == "time":
                df = df.select(
                    col("DateID").cast("int"),
                    to_date("Date").alias("Date"),
                    col("Week"),
                    col("Month"),
                    col("Quarter"),
                    col("Year")
                ).dropDuplicates(["DateID"])
                df.show(1, truncate=False)
                write_to_redshift(df, batch_id, config['dim_time_table'])
        except Exception as e:
            print(f"Error processing {entity} batch {batch_id}: {e}")
    return entity_stream.writeStream \
    .foreachBatch(batch_writer) \
    .outputMode("append") \
    .start()

# Start all streaming queries
queries = [handle_entity(entity) for entity in schemas.keys()]

# Wait for any to terminate
spark.streams.awaitAnyTermination()
