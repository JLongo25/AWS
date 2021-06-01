#!/bin/python3
import boto3
import pandas as pd
import os
import gzip
from datetime import datetime, timedelta
from elasticsearch import helpers
from es_connect import connect_elasticsearch
from s3_helper import access_plog, secret_plog


def elastic_dict():
    df = pd.DataFrame(pd_list, columns=pd_list[0])
    df = df.drop(0)
    df.rename(columns={'version': 'vers'}, inplace=True)

    # Tuple allows for .to_dict() while iterating a Dataframe
    for i, row in df.iterrows():
        x = {**row.to_dict()}
        # Remove any keys as data entries
        if 'start' in x['start']:
            pass
        else:
            yield x


def file_gen():
    for obj in bucket.objects.filter(Delimiter='/', Prefix=f'AWSLogs/201401835213/vpcflowlogs/us-west-2/{yesterday.strftime("%Y/%m/%d")}/'):
        path, filename = os.path.split(obj.key)
        bucket.download_file(obj.key, f'/opt/tcgt-scripts/aws/repo/{filename}')
        yield filename


today = datetime.today()
yesterday = today - timedelta(days=1)
s3 = boto3.resource(
    's3',
    aws_access_key_id=access_plog(),
    aws_secret_access_key=secret_plog()
)

# AWS bucket where log files live
bucket = s3.Bucket('tech-logging')

# This list makes creating Dataframe / dictionary keys easier
pd_list = []
c = 0
for file in file_gen():
    file_path = f'/opt/tcgt-scripts/aws/repo/{file}'
    try:
        with gzip.open(file_path, 'rt') as f:
            for line in f.readlines():
                file = line.split(' ')
                pd_list.append(file)
                # Remove any internal -> internal connections
                if '172' in pd_list[c][3] and '172' in pd_list[c][4]:
                    pd_list.pop(c)
                    c -= 1
                c += 1
    # Only prints if download is done before second half of script as more logs are picked up than were downloaded.
    except FileNotFoundError:
        print(file)
        pass

# Send generator data straight to elastic
es = connect_elasticsearch()
helpers.bulk(es, elastic_dict(), index='aws_logs')
