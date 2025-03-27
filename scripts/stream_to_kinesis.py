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
with open(Path(__file__).parent.parent / 'config/aws_config.yaml', 'r') as file:
    aws_config = yaml.safe_load(file)

AWS_REGION = aws_config['aws_region']
KINESIS_STREAM = aws_config['kinesis_stream']
S3_BUCKET = aws_config['s3_bucket']
S3_PREFIX = aws_config['s3_prefix']
BATCH_SIZE = 100

kinesis = boto3.client('kinesis', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

# Files to stream (reflecting the full schema)
DATA_DIR = Path(__file__).parent / 'sample_data'
data_files = [
    'customers.csv',
    'orders.csv',
    'order_details.csv',
    'order_status_history.csv',
    'order_promotions.csv',
    'promotions.csv',
    'payments.csv',
    'shipments.csv',
    'products.csv',
    'inventory.csv',
    'product_suppliers.csv',
    'suppliers.csv'
]

def stream_and_backup(entity, file_path):
    print(f'Streaming {entity} data from {file_path} to Kinesis...')

    batch = []

    with open(file_path, 'r') as f:
        headers = f.readline().strip().split(',')
        for line in f:
            fields = line.strip().split(',')
            record = dict(zip(headers, fields))
            record['record_type'] = entity  


            # Stream to Kinesis
            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record),
                PartitionKey=str(random.randint(1, 10))  # TODO: improve partitioning
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
