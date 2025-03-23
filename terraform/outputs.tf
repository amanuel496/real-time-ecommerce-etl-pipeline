output "s3_bucket_name" {
  value = aws_s3_bucket.raw_data_bucket.id
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.data_stream.name
}