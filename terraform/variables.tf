variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-west-2"
}

variable "s3_bucket_name" {
  type        = string
  description = "S3 bucket for raw data"
}

variable "kinesis_stream_name" {
  type        = string
  description = "Kinesis stream for real-time ingestion"
}
