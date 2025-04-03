from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp, lit, sum as spark_sum
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import yaml
from pathlib import Path
from datetime import datetime

# Load configuration
def load_config():
    config_path = Path(__file__).parent.parent.parent / 'config/aws_config.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# Define schemas for incoming data
schemas = {
    "orders": StructType()
        .add("OrderID", StringType())
        .add("CustomerID", StringType())
        .add("OrderDate", StringType())
        .add("TotalAmount", DoubleType())
        .add("Status", StringType())
        .add("record_type", StringType()),

    "order_details": StructType()
        .add("OrderDetailID", StringType())
        .add("OrderID", StringType())
        .add("ProductID", StringType())
        .add("Quantity", IntegerType())
        .add("Subtotal", DoubleType())
        .add("record_type", StringType()),

    "order_status_history": StructType()
        .add("OrderStatusID", StringType())
        .add("OrderID", StringType())
        .add("Status", StringType())
        .add("StatusDate", StringType())
        .add("record_type", StringType()),

    "order_promotions": StructType()
        .add("OrderPromotionID", StringType())
        .add("OrderID", StringType())
        .add("PromotionID", StringType())
        .add("DiscountApplied", DoubleType())
        .add("record_type", StringType()),

    "payments": StructType()
        .add("PaymentID", StringType())
        .add("OrderID", StringType())
        .add("PaymentMethod", StringType())
        .add("PaymentStatus", StringType())
        .add("Amount", DoubleType())
        .add("record_type", StringType()),

    "inventory": StructType()
        .add("InventoryID", StringType())
        .add("ProductID", StringType())
        .add("StockLevel", IntegerType())
        .add("LastUpdated", StringType())
        .add("record_type", StringType()),

    "shipments": StructType()
        .add("ShipmentID", StringType())
        .add("OrderID", StringType())
        .add("Carrier", StringType())
        .add("TrackingNumber", StringType())
        .add("Status", StringType())
        .add("DeliveryDate", StringType())
        .add("record_type", StringType())
}

# Start Spark session
spark = SparkSession.builder \
    .appName("WarehouseOrderInventoryStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kinesis stream
raw_stream_df = spark.readStream \
    .format("aws-kinesis") \
    .option("kinesis.streamName", config['kinesis_stream']) \
    .option("kinesis.startingPosition", "TRIM_HORIZON") \
    .option("kinesis.awsAccessKeyId", config['aws_access_key_id']) \
    .option("kinesis.awsSecretKey", config['aws_secret_access_key']) \
    .option("kinesis.endpointUrl", config['kinesis_endpoint_url']) \
    .option("kinesis.maxFetchTimeInMs", "1000") \
    .option("kinesis.maxFetchRecordsPerShard", "100") \
    .load()

json_df = raw_stream_df.selectExpr("CAST(data AS STRING) as data")

# Write to Redshift
def write_to_redshift(df, batch_id, table_name):
    print(f"Writing batch {batch_id} to {table_name}...")
    df.write \
        .format("jdbc") \
        .option("url", config['redshift_config']['url']) \
        .option("dbtable", table_name) \
        .option("user", config['redshift_config']['user']) \
        .option("password", config['redshift_config']['password']) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("append") \
        .save()

# Helper to extract and cache record types
def extract_stream(entity):
    return json_df \
        .select(from_json(col("data"), schemas[entity]).alias("record")) \
        .filter(col("record.record_type") == entity) \
        .select("record.*")

# Start streaming per entity
for entity in ["orders", "order_details", "order_promotions", "payments", "inventory", "order_status_history", "shipments"]:
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    entity_stream = extract_stream(entity)
    entity_stream.writeStream \
        .format("memory") \
        .queryName(entity) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", f"/tmp/spark-checkpoints/{entity}/{timestamp}") \
        .start()

    entity_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

# Wait for all memory streams to initialize
spark.streams.awaitAnyTermination(timeout=840)

# Manual batch write logic for fact tables (after stream runs for a bit)
def batch_write_fact_tables():
    try:
        orders_df = spark.sql("SELECT * FROM orders").cache()
        if orders_df.rdd.isEmpty():
            print("Orders batch is empty. Skipping.")
            return

        details_df = spark.sql("SELECT * FROM order_details")
        payments_df = spark.sql("SELECT * FROM payments")
        promos_df = spark.sql("SELECT * FROM order_promotions")
        shipments_df = spark.sql("SELECT * FROM shipments")

        dim_time_df = spark.read \
            .format("jdbc") \
            .option("url", config['redshift_config']['url']) \
            .option("dbtable", config['redshift_config']['tables']['dim_time']) \
            .option("user", config['redshift_config']['user']) \
            .option("password", config['redshift_config']['password']) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .load()

        enriched_df = orders_df.alias("o") \
            .join(details_df.alias("d"), col("o.OrderID") == col("d.OrderID"), "left") \
            .join(promos_df.alias("p"), col("o.OrderID") == col("p.OrderID"), "left") \
            .join(payments_df.alias("pm"), col("o.OrderID") == col("pm.OrderID"), "left") \
            .join(shipments_df.alias("s"), col("o.OrderID") == col("s.OrderID"), "left") \
            .join(dim_time_df.alias("dt"), to_date("o.OrderDate") == col("dt.Date"), "left") \
            .select(
                col("o.OrderID").cast("string"),
                col("o.CustomerID").cast("string"),
                col("d.ProductID").cast("string"),
                col("p.PromotionID").cast("string").alias("PromoID"),
                col("dt.DateID").cast("int").alias("DateID"),
                col("o.TotalAmount"),
                col("p.DiscountApplied"),
                col("pm.PaymentMethod"),
                col("pm.PaymentStatus"),
                to_date("s.DeliveryDate").alias("DeliveryDate"),
                col("o.Status").alias("OrderStatus")
            ).dropDuplicates(["OrderID", "ProductID"])

        enriched_df.show(1, truncate=False)
        write_to_redshift(enriched_df, 0, config['redshift_config']['tables']['fact_orders'])

    except Exception as e:
        print(f"Error processing orders batch: {e}")

    try:
        print("Processing inventory batch...")
        inventory_df = spark.sql("SELECT * FROM inventory")
        if inventory_df.rdd.isEmpty():
            print("Inventory batch is empty. Skipping.")
            return

        dim_products_df = spark.read \
            .format("jdbc") \
            .option("url", config['redshift_config']['url']) \
            .option("dbtable", config['redshift_config']['tables']['dim_products']) \
            .option("user", config['redshift_config']['user']) \
            .option("password", config['redshift_config']['password']) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .load()

        enriched_inventory = inventory_df.alias("inv") \
            .join(dim_products_df.alias("ps"), col("inv.ProductID") == col("ps.ProductID"), "left") \
            .select(
                col("inv.InventoryID").cast("string"),
                col("inv.ProductID").cast("string"),
                col("ps.SupplierID").cast("string"),
                col("inv.StockLevel").cast("int"),
                to_timestamp("inv.LastUpdated").alias("LastUpdated")
            )

        enriched_inventory.show(1, truncate=False)
        write_to_redshift(enriched_inventory, 0, config['redshift_config']['tables']['fact_inventory'])

    except Exception as e:
        print(f"Error processing inventory batch: {e}")

batch_write_fact_tables()
