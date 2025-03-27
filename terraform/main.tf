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

# Redshift Serverless Namespace
resource "aws_redshiftserverless_namespace" "etl_namespace" {
  namespace_name = var.redshift_namespace
  admin_username = var.redshift_username
  admin_user_password = var.redshift_password
}

# Redshift Serverless Workgroup
resource "aws_redshiftserverless_workgroup" "etl_workgroup" {
  workgroup_name = var.redshift_workgroup
  namespace_name = aws_redshiftserverless_namespace.etl_namespace.namespace_name
  publicly_accessible = true

  tags = {
    Name        = "ETL Redshift Workgroup"
    Environment = "Dev"
  }
}

# Redshift Security Group (Allow Access from anywhere for demo purposes)
resource "aws_security_group" "redshift_sg" {
  name        = "redshift_sg"
  description = "Allow Redshift access"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "redshift-sg"
  }
}

# Execute SQL script to create Redshift tables
resource "aws_redshiftdata_statement" "create_redshift_tables" {
  database   = "dev"
  sql        = file("${path.module}/../redshift/create_tables.sql")
  workgroup_name = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
  depends_on = [aws_redshiftserverless_workgroup.etl_workgroup]
}
