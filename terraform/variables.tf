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

variable "redshift_namespace" {
  type        = string
  description = "Redshift Serverless namespace"
}

variable "redshift_workgroup" {
  type        = string
  description = "Redshift Serverless workgroup"
}

variable "redshift_username" {
  type        = string
  description = "Redshift admin username"
}

variable "redshift_password" {
  type        = string
  description = "Redshift admin password"
  sensitive   = true
}

variable "vpc_id" {
  type        = string
  description = "VPC ID for Redshift security group"
}