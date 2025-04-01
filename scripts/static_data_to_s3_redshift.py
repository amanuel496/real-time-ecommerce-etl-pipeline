import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
from uuid import uuid4
from pathlib import Path
import yaml
import psycopg2
import logging
from decimal import Decimal, getcontext

# Set decimal context precision
getcontext().prec = 10

# Load config
with open(Path(__file__).parent.parent / 'config/aws_config.yaml', 'r') as file:
    aws_config = yaml.safe_load(file)

AWS_REGION = aws_config['aws_region']
S3_BUCKET = aws_config['s3']['bucket']
S3_PREFIX = aws_config['s3']['prefix']
AWS_ACCESS_KEY = aws_config['aws_access_key_id']
AWS_SECRET_KEY = aws_config['aws_secret_access_key']
REDSHIFT_CONFIG = aws_config['redshift_config']

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
logger.info("Loaded AWS credentials from config and initialized S3 client.")

DATA_DIR = Path(__file__).parent / 'sample_data'

all_entities = [
    'customers', 'products', 'suppliers', 'product_suppliers', 'promotions', 'shipments'
]

entity_schemas = {
    "customers": {
        "CustomerID": "string",
        "Name": "string",
        "Email": "string",
        "Address": "string",
        "SignupDate": "datetime64[ns]"
    },
    "products": {
        "ProductID": "string",
        "Name": "string",
        "Category": "string",
        "Price": "float64"
    },
    "suppliers": {
        "SupplierID": "string",
        "Name": "string",
        "ContactInfo": "string"
    },
    "product_suppliers": {
        "ProductSupplierID": "string",
        "ProductID": "string",
        "SupplierID": "string"
    },
    "promotions": {
        "PromoID": "string",
        "Discount": "float64",
        "StartDate": "datetime64[ns]",
        "EndDate": "datetime64[ns]",
        "CampaignType": "string"
    },
    "shipments": {
        "ShipmentID": "string",
        "OrderID": "string",
        "Carrier": "string",
        "TrackingNumber": "string",
        "Status": "string",
        "DeliveryDate": "datetime64[ns]"
    }
}

def cast_dataframe(df, entity):
    schema = entity_schemas.get(entity, {})
    for col, dtype in schema.items():
        if col not in df.columns:
            logger.warning(f"{entity}: Missing expected column '{col}'")
            continue
        try:
            if "datetime" in dtype:
                df[col] = pd.to_datetime(df[col]).dt.date
            elif dtype == "string":
                df[col] = df[col].astype("string")
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            logger.error(f"{entity}: Failed to cast {col} to {dtype} – {e}")
    logger.info(f"{entity} column types:\n{df.dtypes}")
    logger.info(f"{entity} column order: {list(df.columns)}")
    return df

def upload_to_s3(df, entity):
    # Override schema for Redshift decimal compatibility
    schema_override = None

    if entity == "dim_products":
        schema_override = pa.schema([
            pa.field("ProductID", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Category", pa.string()),
            pa.field("Price", pa.decimal128(10, 2)),
            pa.field("SupplierID", pa.string()) 
        ])
    elif entity == "dim_promotions":
        schema_override = pa.schema([
            pa.field("PromoID", pa.string()),
            pa.field("Discount", pa.decimal128(5, 2)),
            pa.field("StartDate", pa.date32()),
            pa.field("EndDate", pa.date32()),
            pa.field("CampaignType", pa.string())
        ])

    table = pa.Table.from_pandas(df, schema=schema_override)

    logger.info(f"{entity} Parquet schema:")
    for field in table.schema:
        logger.info(f" - {field.name}: {field.type}")

    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')

    now = datetime.now()
    # Strip "dim_" prefix if present
    s3_entity = entity[4:] if entity.startswith("dim_") else entity

    s3_key = (
        f'{S3_PREFIX}/{s3_entity}/'
        f'year={now.year}/month={now.month:02d}/day={now.day:02d}/'
        f'{uuid4().hex}.parquet'
)

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
        logger.info(f"Uploaded {entity} to S3 at s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Failed to upload {entity} to S3: {e}")
        return None

def copy_to_redshift(table_name, s3_key):
    try:
        copy_sql = f"""
            COPY {table_name}
            FROM 's3://{S3_BUCKET}/{s3_key}'
            ACCESS_KEY_ID '{AWS_ACCESS_KEY}'
            SECRET_ACCESS_KEY '{AWS_SECRET_KEY}'
            FORMAT AS PARQUET;
        """
        conn = psycopg2.connect(
            dbname=REDSHIFT_CONFIG['database'],
            user=REDSHIFT_CONFIG['user'],
            password=REDSHIFT_CONFIG['password'],
            host=REDSHIFT_CONFIG['host'],
            port=REDSHIFT_CONFIG['port']
        )
        with conn.cursor() as cur:
            cur.execute(copy_sql)
            conn.commit()
            logger.info(f"Copied data to Redshift table: {table_name}")
        conn.close()
    except Exception as e:
        logger.error(f"Redshift COPY failed for {table_name}: {e}")

def process_and_load_static_data():
    dfs = {}

    # Load, cast, and upload base entities
    for entity in all_entities:
        file_path = DATA_DIR / f"{entity}.csv"
        if not file_path.exists():
            logger.warning(f"Missing CSV: {file_path}")
            continue
        df = pd.read_csv(file_path)
        df = cast_dataframe(df, entity)
        dfs[entity] = df
        upload_to_s3(df, entity)

    # dim_products
    if {"products", "product_suppliers"}.issubset(dfs):
        df_products = dfs["products"]
        df_ps = dfs["product_suppliers"]

        logger.info("Merging products with product_suppliers...")
        dim_products = pd.merge(df_products, df_ps[["ProductID", "SupplierID"]], on="ProductID", how="left")

        # Debug: Check if SupplierID exists
        if "SupplierID" not in dim_products.columns:
            logger.error("SupplierID column missing in dim_products after merge.")
            logger.error(f"dim_products columns: {dim_products.columns.tolist()}")
            return
        else:
            logger.info("SupplierID column exists in dim_products.")

        expected_columns = ["ProductID", "Name", "Category", "Price", "SupplierID"]
        missing_cols = [col for col in expected_columns if col not in dim_products.columns]
        if missing_cols:
            logger.error(f"Missing expected columns: {missing_cols}")
            return

        dim_products = dim_products[expected_columns]

        # Format price as Decimal
        dim_products["Price"] = dim_products["Price"].apply(
            lambda x: Decimal(f"{x:.2f}") if pd.notnull(x) else None
        )

        logger.info(f"Sample dim_products row:\n{dim_products.head(1)}")

        s3_key = upload_to_s3(dim_products, "dim_products")
        if s3_key:
            copy_to_redshift("dim_products", s3_key)

    # dim_suppliers
    if {"suppliers", "product_suppliers"}.issubset(dfs):
        df_suppliers = dfs["suppliers"]
        supplier_ids = dfs["product_suppliers"]["SupplierID"].unique()
        dim_suppliers = df_suppliers[df_suppliers["SupplierID"].isin(supplier_ids)]
        dim_suppliers = dim_suppliers[["SupplierID", "Name", "ContactInfo"]]
        s3_key = upload_to_s3(dim_suppliers, "dim_suppliers")
        if s3_key:
            copy_to_redshift("dim_suppliers", s3_key)

    # dim_customers
    if "customers" in dfs:
        dim_customers = dfs["customers"][["CustomerID", "Name", "Email", "Address", "SignupDate"]]
        s3_key = upload_to_s3(dim_customers, "dim_customers")
        if s3_key:
            copy_to_redshift("dim_customers", s3_key)

    # dim_promotions
    if "promotions" in dfs:
        df_promotions = dfs["promotions"].copy()
        df_promotions["Discount"] = df_promotions["Discount"].apply(
            lambda x: Decimal(f"{x:.2f}") if pd.notnull(x) else None
        )
        df_promotions["StartDate"] = pd.to_datetime(df_promotions["StartDate"]).dt.date
        df_promotions["EndDate"] = pd.to_datetime(df_promotions["EndDate"]).dt.date
        df_promotions["PromoID"] = df_promotions["PromoID"].astype(str)
        df_promotions["CampaignType"] = df_promotions["CampaignType"].astype(str)
        dim_promotions = df_promotions[["PromoID", "Discount", "StartDate", "EndDate", "CampaignType"]]
        s3_key = upload_to_s3(dim_promotions, "dim_promotions")
        if s3_key:
            copy_to_redshift("dim_promotions", s3_key)

if __name__ == '__main__':
    process_and_load_static_data()
