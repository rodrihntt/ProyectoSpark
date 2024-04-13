import boto3
import botocore.config

# Crear una configuración personalizada con un tiempo de espera más largo
config = botocore.config.Config(connect_timeout=120, read_timeout=200)

# Create an S3 client instance
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # LocalStack endpoint URL
    aws_access_key_id='test',  # dummy access key (LocalStack default)
    aws_secret_access_key='test',  # dummy secret key (LocalStack default)
    config=config
)

# Define the bucket name
bucket_name = 'bucket-rodrigo'

# Create the bucket
s3.create_bucket(Bucket=bucket_name)

print(f"Bucket '{bucket_name}' created successfully.")

# Define the bucket name
bucket_name = 'bucket-rodrigo-clean'

# Create the bucket
s3.create_bucket(Bucket=bucket_name)

print(f"Bucket '{bucket_name}' created successfully.")
