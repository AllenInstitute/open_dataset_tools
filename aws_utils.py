import boto3
import os

__all__ = ['get_aws_keys', 'get_boto3_session']

def get_aws_keys(filename):
    """
    Read and return the 'Access key ID' and 'Secret access key'
    from an AWS credentials csv file

    Parameters
    ----------
    filename is the path to the csv file containing the credentials

    Returns
    -------
    Access key ID
    Secret access key
    """
    if filename is None:
        raise RuntimeError("Must specify filename in get_aws_keys")

    if not os.path.isfile(filename):
        raise RuntimeError("\n%s\nis not a file" % filename)

    with open(filename, 'r') as in_file:
        header = in_file.readline().strip().split(',')
        id_dex = None
        key_dex = None
        for ii, h in enumerate(header):
            if h == 'Access key ID':
                id_dex = ii
            if h == 'Secret access key':
                key_dex = ii
        if id_dex is None or key_dex is None:
            msg = '\n'
            if id_dex is None:
                msg += "Could not find 'Access key ID'\n"
            if key_dex is None:
                msg += "Could not find 'Secret access key'\n"

            msg += "in '%s'\n" % filename
            raise RuntimeError(msg)

        p = in_file.readline().strip().split(',')
        aws_key_id = p[id_dex]
        secret_key = p[key_dex]

    return aws_key_id, secret_key


def get_boto3_session(filename=None):
    """
    Open a boto3.Session

    Parameter
    ---------
    filename is the path to the csv with AWS credentials. Defaults to None.
    If filename==None, will look in `accessKeys.csv'
    """
    if filename is None:
        filename = 'accessKeys.csv'
    aws_id, aws_secret = get_aws_keys(filename)
    session = boto3.Session(aws_access_key_id=aws_id,
                            aws_secret_access_key=aws_secret)

    return session
