import boto3
import urllib.request
import logging
import os

logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    region = "eu-west-1"
    s3 = boto3.client("s3", region_name=region)

    year = event["year"]
    month = event["month"]
    day = event["day"]
    env = os.getenv("ENV")

    data = f"https://storage.googleapis.com/ked-app-prd.appspot.com/{year}/{month}/{day}/schedule.json"
    bucket = f"{env}-ked-kube-datalake"
    key = f"data/raw/ked/schedule/schedule_{year}_{month}_{day}.txt"

    logging.info(f"Downloading data from {data}")
    contents = urllib.request.urlopen(data).read()
    logging.info("Download finished")

    logging.info(f"Uploading to s3://{bucket}/{key}")
    s3.put_object(Body=contents, Bucket=bucket, Key=key)
    logging.info("Upload finished")

