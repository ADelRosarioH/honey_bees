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
    
    print('{}: {}'.format(bucket, key))

    path, ext = os.path.splitext(key)

    temp = tempfile.NamedTemporaryFile(prefix='{}.'.format(os.path.basename(path)), suffix=ext)

    # download
    print('Starting download...')
    try:
        s3.download_file(bucket, key, temp.name)    
    except Exception as e:
        raise e

    print('Object saved to: {}'.format(temp.name))

    # parse
    print('Starting parse...')

    outputs = []

    try:
        outputs = parser.pdf_to_csv_b(temp.name)
    except Exception as e:
        raise e

    for output in outputs:
        print('Object parsed to: {}'.format(output))

    # upload
    print('Starting upload...')
    
    try:
        for output in outputs:
            output_file_name = os.path.join(os.path.dirname(path), os.path.basename(output))

            if os.path.exists(output):
                response = s3.upload_file(output, bucket, output_file_name)
                
                print('Object uploaded to: {}'.format(output_file_name))

    except Exception as e:
        raise e

    return
