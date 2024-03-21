import boto3

def create_s3_bucket(bucket_name, region):
    # Crea un cliente de S3
    s3_client = boto3.client('s3', region_name=region)

    # Crea el bucket
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region  # Especifica la región para el bucket
            }
        )
        print("Bucket creado exitosamente.")
    except Exception as e:
        print("Error al crear el bucket:", e)

if __name__ == "__main__":
    # Especifica el nombre y la región del bucket que deseas crear
    bucket_name = "bucketRodrigo"
    region = "us-east-1"  # Cambia esto a tu región deseada
    
    # Llama a la función para crear el bucket
    create_s3_bucket(bucket_name, region)