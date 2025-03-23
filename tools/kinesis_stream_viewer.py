import boto3
import yaml
import time
import json
from pathlib import Path
from rich.console import Console
from rich.table import Table

# Load AWS config
with open(Path(__file__).parents[1] / "config/aws_config.yaml", "r") as file:
    config = yaml.safe_load(file)

region = config["aws_region"]
stream_name = config["kinesis_stream"]

kinesis = boto3.client("kinesis", region_name=region)
console = Console()

# Get shard ID
stream_desc = kinesis.describe_stream(StreamName=stream_name)
shard_id = stream_desc["StreamDescription"]["Shards"][0]["ShardId"]

# Get shard iterator (TRIM_HORIZON = from the start, LATEST = only new)
iterator = kinesis.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"
)["ShardIterator"]

console.print(f"\n🎥 [bold green]Watching stream:[/bold green] {stream_name} [dim](shard: {shard_id})[/dim]\n")

# Live viewer loop
try:
    while True:
        response = kinesis.get_records(ShardIterator=iterator, Limit=10)
        records = response["Records"]
        iterator = response["NextShardIterator"]

        if records:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("PartitionKey")
            table.add_column("Data")

            for r in records:
                payload = json.loads(r["Data"])
                table.add_row(r["PartitionKey"], json.dumps(payload))

            console.print(table)

        time.sleep(1)
except KeyboardInterrupt:
    console.print("\nExiting viewer...")
