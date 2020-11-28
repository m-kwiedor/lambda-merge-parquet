import boto3
import botocore
import shutil
import logging
import os

from fastparquet import ParquetFile, write
from fastparquet import write

from datetime import datetime, tzinfo

BUCKET = os.environ['S3_BUCKET']
TTL_MINUTES = os.environ['TTL_MINUTES']

LOG_LEVEL = logging.INFO

log = logging.getLogger()
log.setLevel(LOG_LEVEL)

s3 = boto3.resource('s3')
bucket = s3.Bucket(BUCKET)

def get_next_merge_id(folder):
    i = 0
    merge = True

    while merge:
        try:
            s3.meta.client.head_object(Bucket = BUCKET, Key=f'{folder}merged-{i}.parquet')
            i = i + 1
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                merge = False

    return i

def conv_secs_to_minutes(seconds):
    return seconds/60

def should_merge(last_modified, ttl):
    now = datetime.utcnow()
    diff = now.replace(tzinfo=None) - last_modified.replace(tzinfo=None)
    diffseconds = diff.total_seconds()
    diffminutes = conv_secs_to_minutes(diffseconds)
    return diffminutes > int(ttl)

def merge_parquet(folder, items):
    try:
        files = []

        log.debug(f'Processing {folder}')

        merge = get_next_merge_id(folder)

        log.debug(f'Creating merge file merged-{merge}.parquet')

        if not os.path.exists('/tmp/{}'.format(os.path.dirname(folder))):
            os.makedirs('/tmp/{}'.format(os.path.dirname(folder)))

        file_list = []

        # Download Files to tmp
        for item in items:
            s3.meta.client.download_file(BUCKET, item.key, '/tmp/{}'.format(item.key))
            file_list.append('/tmp/{}'.format(item.key))

        pf = ParquetFile(file_list)

        write(f'/tmp/{folder}merged-{merge}.parquet', pf.to_pandas())
        log.debug(f'Created parquet merged file - Uploading to {BUCKET}')

        s3.meta.client.upload_file(f'/tmp/{folder}merged-{merge}.parquet', BUCKET, f'{folder}merged-{merge}.parquet')
        shutil.rmtree(f'/tmp/{folder}')

        for item in items:
            s3.meta.client.delete_object(Bucket = BUCKET, Key = item.key)
    except Exception as e:
        print(e)

def lambda_handler(event, context):
    log.info('Getting all objects from Bucket %s', BUCKET)

    object_iterator = bucket.objects.all()

    directories = {}

    mergecount = 0
    for item in object_iterator:
        if ".parquet" in item.key:
            shouldMerge = should_merge(item.last_modified, TTL_MINUTES)
            if shouldMerge:
                dirname = os.path.dirname(item.key) + '/'

                if dirname not in directories:
                    directories[dirname] = []

                directories[dirname].append(item)
                mergecount = mergecount + 1

    for directory in directories:
        merge_parquet(directory, directories[directory])
       