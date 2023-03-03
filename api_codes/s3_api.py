import os
import sys
import boto3
from fastapi import APIRouter, Depends, FastAPI
from pydantic import BaseModel
import botocore
from backend import oauth2, schema


cwd = os.getcwd()
project_dir = os.path.abspath(os.path.join(cwd, '..'))
sys.path.insert(0, project_dir)
os.environ['PYTHONPATH'] = project_dir + ':' + os.environ.get('PYTHONPATH', '')

from backend import nexrad_main




def create_connection():

    """
    Create a connection to AWS S3 bucket

    Returns:
        s3client: boto3 client object

    """

    s3client = boto3.client('s3',
    region_name= "us-east-1",
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY1'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY1'))

    return s3client

# app =FastAPI()

router = APIRouter()

@router.get('/s3_fetch_keys')
async def s3_fetch_keys(fn_s3_fetch_keys: schema.fn_s3_fetch_keys, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    """
    This function fetches the keys from the S3 bucket based on the bucket name

    :param fn_s3_fetch_keys: bucket_name
    :return: keys
    """

    s3client = create_connection()

    try:
        objects = s3client.list_objects_v2(Bucket=fn_s3_fetch_keys.bucket_name)

        files = []
        for obj in objects.get("Contents", []):
            nexrad_main.write_logs("Status: 200, Message: keys fetched from the  S3")
            files.append(obj.get("Key"))
        
        nexrad_main.write_logs("Status: 200, Message: keys fetched from the  S3")
        return {"Keys" : files}
    except Exception as e:
        nexrad_main.write_logs("Status: 400, Message: keys not fetched from the  S3")
        return {"Error" : str(e)}


@router.get('/download_s3_file')
async def download_s3_file(fn_s3_download_file: schema.fn_s3_download_file, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):

    """
    This function downloads the file from the S3 bucket based on the file name and bucket name

    :param fn_s3_download_file: bucket_name, file_name
    :return: file_name
    """

    s3client = create_connection()
    objects = s3client.list_objects_v2(Bucket=fn_s3_download_file.bucket_name)
    if not any(obj.get("Key") == fn_s3_download_file.file_name for obj in objects.get("Contents", [])):
        s3client.download_file(fn_s3_download_file.bucket_name, fn_s3_download_file.file_name, fn_s3_download_file.file_name)
        nexrad_main.write_logs("Status: 200, Message: file downloaded from the  S3" + fn_s3_download_file.file_name)
    else:
        nexrad_main.write_logs("Status: 400, Message: file not found in the  S3" + fn_s3_download_file.file_name)
        return {"Error" : "File not found"}
    
