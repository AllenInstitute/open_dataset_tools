import boto3
from botocore import UNSIGNED
from botocore.client import Config


def get_public_boto3_client():
    """Convenience function to return a boto3 client that can access
    publically available AWS services (like public S3 buckets) without
    any AWS credentials.

    Returns
    -------
    A boto3 client instance.
    """
    public_client = boto3.client(
        's3', config=Config(signature_version=UNSIGNED)
    )
    return public_client
