"""
Creates landing zone bucket
"""

import os

import boto3

if __name__ == "__main__":
    s3 = boto3.resource(
        "s3",
        endpoint_url=os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
        aws_access_key_id=os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"],
        aws_session_token=None,
        verify=False,
    )
    s3.create_bucket(Bucket="landingzone")
