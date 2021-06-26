import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from io import BytesIO
import logging
from botocore.exceptions import ClientError


# Returns csv files to be converted to pq format
def read_csv(s3,bucket):
    response = s3.list_objects(Bucket=bucket)
    contents = response['Contents']
    keys = []
    csv_keys = []
    for content in contents:
        keys.append(content['Key'])
        if content['Key'].endswith('.csv'):
            csv_keys.append(content['Key'])

    return csv_keys


def csv_to_pq_s3(s3, csv_files):
    try:
        for csv in csv_files:
            file = s3.get_object(Bucket=bucket, Key=csv)
            df = pd.read_csv(io.BytesIO(file['Body'].read()))
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            filename = csv.split(".csv")[0]
            pq_file = filename + '.parquet'
            response = s3.put_object(Body=out_buffer.getvalue(),
                                     Bucket=bucket,
                                     Key=pq_file)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def csv_to_pq_local(csv_files):
    for csv in csv_files:
        df = pd.read_csv(csv)
        filename = csv.split(".csv")[0]
        pq_file = filename + '.parquet'
        table = pa.Table.from_pandas(df)
        pq.write_table(table, pq_file)
    return True


if __name__ == '__main__':

    # S3 client
    s3 = boto3.client('s3',
                      region_name='us-west-2',
                      aws_access_key_id='key',
                      aws_secret_access_key='secret'
                      )

    # Input data in csv format is stored in the below bucket
    bucket = 'airline-data'

    # Data is split with states as prefixes and yyyy_mm as name
    # E.g ca-2019-12 contains airline data for CA for Dec 2019

    csv_files = read_csv(s3,bucket)

    if csv_files:
        csv_to_pq_s3(s3, csv_files)
