import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
from uuid import uuid4
from pathlib import Path
import yaml
import json
import time
import random

# Load AWS config
with open(Path(__file__).parent / 'aws_config.yaml', 'r') as file:
    aws_config = yaml.safe_load(file)

AWS_REGION = aws_config['aws_region']
AWS_ACCESS_KEY = aws_config['aws_access_key_id']
AWS_SECRET_KEY = aws_config['aws_secret_access_key']
KINESIS_STREAM = aws_config['kinesis_stream']
S3_BUCKET = aws_config['s3']['bucket']
S3_PREFIX = aws_config['s3']['prefix']
BATCH_SIZE = 100

kinesis = boto3.client(
    'kinesis',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

DATA_DIR = Path(__file__).parent / 'sample_data'
data_files = [
    'orders.csv',
    'order_details.csv',
    'order_status_history.csv',
    'order_promotions.csv',
    'payments.csv',
    'inventory.csv'
]

def stream_and_backup(entity, file_path):
    print(f'Streaming {entity} data from {file_path} to Kinesis...')

    batch = []

    with open(file_path, 'r') as f:
        headers = f.readline().strip().split(',')
        for line in f:
            fields = line.strip().split(',')
            record = {key: value.strip() for key, value in zip(headers, fields)}
            record['record_type'] = entity

            try:
                if entity == 'orders':
                    record['TotalAmount'] = float(record.get('TotalAmount', 0.0))
                    if not record.get('OrderID') or not record.get('CustomerID'):
                        raise ValueError("Missing required OrderID or CustomerID.")

                elif entity == 'order_details':
                    record['Quantity'] = int(record.get('Quantity', 0))
                    record['Subtotal'] = float(record.get('Subtotal', 0.0))
                    if not record.get('OrderDetailID') or not record.get('OrderID'):
                        raise ValueError("Missing OrderDetailID or OrderID.")

                elif entity == 'order_status_history':
                    if not record.get('OrderStatusID') or not record.get('OrderID') or not record.get('StatusDate'):
                        raise ValueError("Missing OrderStatusID, OrderID, or StatusDate.")
                    datetime.strptime(record['StatusDate'], "%Y-%m-%d")

                elif entity == 'order_promotions':
                    record['DiscountApplied'] = float(record.get('DiscountApplied', 0.0))
                    if not record.get('OrderPromotionID') or not record.get('PromotionID'):
                        raise ValueError("Missing IDs in promotion record.")

                elif entity == 'payments':
                    record['Amount'] = float(record.get('Amount', 0.0))
                    if not record.get('PaymentID') or not record.get('OrderID'):
                        raise ValueError("Missing PaymentID or OrderID.")

                elif entity == "inventory":
                    try:
                        record["StockLevel"] = int(record["StockLevel"])
                        record["LastUpdated"] = record["LastUpdated"].split('.')[0]  # Remove microseconds
                    except (ValueError, KeyError) as e:
                        raise ValueError(f"Invalid inventory data: {e}")

            except Exception as e:
                print(f"\u26a0\ufe0f Skipping bad {entity} record: {record}, error: {e}")
                continue

            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record),
                PartitionKey=str(random.randint(1, 10))
            )

            print(f'Streamed record to Kinesis: {record}')
            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                write_batch_to_s3(entity, batch)
                batch = []

            time.sleep(0.1)

    if batch:
        write_batch_to_s3(entity, batch)

def write_batch_to_s3(entity, batch):
    df = pd.DataFrame(batch)
    print(f'The df created from incoming batch: {batch} \n is {df}')

    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')

    now = datetime.now()
    s3_key = (
        f'{S3_PREFIX}/{entity}/'
        f'year={now.year}/month={now.month:02d}/day={now.day:02d}/'
        f'{uuid4().hex}.parquet'
    )

    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f'Batch of {len(batch)} records written to S3 at {s3_key}')

if __name__ == "__main__":
    for filename in data_files:
        entity = filename.split('.')[0]
        path = DATA_DIR / filename
        if path.exists():
            stream_and_backup(entity, path)
        else:
            print(f"File {path} does not exist. Skipping...")

    print("Streaming and backup completed.")
