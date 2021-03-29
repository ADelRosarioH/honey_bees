import os
import logging
import urllib.parse
import boto3
import tempfile

from utils import parser

s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    temp = tempfile.NamedTemporaryFile(prefix=key)
        
    # download file from s3 to temp
    try:
        s3.download_file(bucket, key, temp.name)    
    except Exception as e:
        logging.error('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    output = parser.pdf_to_csv_a(temp.name)

    # upload file from tmp to s3
    try:
        path, ext = os.path.splitext(key)
        file_name = '{}.csv'.format(path)

        response = s3_client.upload_file(file_name, bucket, output)
    except ClientError as e:
        logging.error(e)
        raise e

    return True
