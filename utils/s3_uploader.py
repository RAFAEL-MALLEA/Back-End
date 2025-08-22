import boto3
from botocore.exceptions import NoCredentialsError
from fastapi import UploadFile, HTTPException
import uuid
import os

S3_BUCKET_NAME = "cdn-inventaria"
AWS_REGION = "us-east-1"

s3_client = boto3.client("s3")

def upload_file_to_s3(file: UploadFile) -> str:
    """
    Sube un archivo a S3 y devuelve la URL pública.
    """
    if not S3_BUCKET_NAME:
        raise HTTPException(status_code=500, detail="La configuración de S3 (S3_BUCKET_NAME) no está disponible.")

    # Generar un nombre de archivo único para evitar colisiones
    file_extension = file.filename.split(".")[-1]
    file_key = f"avatars/{uuid.uuid4()}.{file_extension}"

    try:
        s3_client.upload_fileobj(
            file.file,
            S3_BUCKET_NAME,
            file_key
        )
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credenciales de AWS no encontradas.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a S3: {e}")

    file_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{file_key}"
    
    return file_url