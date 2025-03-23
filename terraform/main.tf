provider "aws" {
  region = var.aws_region
}

# S3 Bucket
resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true

  tags = {
    Name        = "Ecommerce Raw Data"
    Environment = "Dev"
  }
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "data_stream" {
  name             = var.kinesis_stream_name
  retention_period = 24

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Name        = "Ecommerce Stream"
    Environment = "Dev"
  }
}
