"""Create the local postingfile bucket and verify an S3 round trip."""
import io
import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

client = boto3.client(
    's3', endpoint_url=os.environ['S3_ENDPOINT_URL'],
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}),
)
for attempt in range(30):
    try:
        client.list_buckets()
        break
    except Exception:
        if attempt == 29:
            raise
        time.sleep(2)
bucket = 'postingfile'
try:
    client.head_bucket(Bucket=bucket)
except ClientError as exc:
    if exc.response['ResponseMetadata']['HTTPStatusCode'] != 404:
        raise
    client.create_bucket(Bucket=bucket)
payload = b'RH|DEMO\nDH|LOCAL S3 STREAM TEST\nRT|1\n'
key = 'demo/QR_RECON_DEMO_ISS'
client.upload_fileobj(io.BytesIO(payload), bucket, key)
response = client.get_object(Bucket=bucket, Key=key)
with response['Body'] as stream:
    assert stream.read() == payload
print(f'PASS: bucket {bucket}; upload/download s3://{bucket}/{key} ({len(payload)} bytes)')
