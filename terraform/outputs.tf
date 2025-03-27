output "s3_bucket_name" {
  value = aws_s3_bucket.raw_data_bucket.id
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.data_stream.name
}

output "redshift_namespace" {
  value = aws_redshiftserverless_namespace.etl_namespace.namespace_name
}

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.etl_workgroup.endpoint
}

output "redshift_database_url" {
  value = "jdbc:redshift://${aws_redshiftserverless_workgroup.etl_workgroup.endpoint}/dev"
}