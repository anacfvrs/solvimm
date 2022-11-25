# coding=utf-8

import boto3
import json
from PIL import Image
import PIL.Image

s3_client = boto3.client('s3')
dyn_client = session.client('dynamodb')
dynamo_table = 'ImagesDynamoDbTable'

def extractMetadata(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        s3_client.download_file(bucket, key, download_path)
    
        with Image.open(download_path) as img:
            dict_metadata = {
                'image_height': {'N': str(img.height)},
                'image_width': {'N': str(img.width)},
                'image_size_bytes': {'N': str(len(img.fp.read()))}
            }
            
            dyn_client.put_item(TableName=dynamo_table, Item=dict_metadata)
       
