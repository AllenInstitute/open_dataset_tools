import os
import hashlib
import json
import aws_utils

def _compare_md5(fname, target):
    """
    Compare the md5 checksum of the file specified by fname to the
    string specified by target. Return boolean result indicating if
    they are equal.
    """
    md5_obj = hashlib.md5()
    with open(fname, 'rb') as in_file:
        for line in in_file:
            md5_obj.update(line)
    return md5_obj.hexdigest()==target


def _get_tmp_dir():
    """
    Check the validity of and return the name of the tmp/
    sub directory of the directory containing metadata_utils.py
    """
    tmp_dir = os.path.dirname(os.path.abspath(__file__))
    tmp_dir = os.path.join(tmp_dir, 'tmp')

    if os.path.exists(tmp_dir) and not os.path.isdir(tmp_dir):
        raise RuntimeError('\n%s\nexists but is not a dir' % tmp_dir)

    return tmp_dir


def get_atlas_metadata(session=None):
    """
    Load the metadata for the entire atlas into memory.
    If you have not already downloaded this file, it will
    be downloaded into the tmp/ sub-directory of the directory
    containing metadata_utils.py

    Parameters
    ----------
    session is a boto3.Session. If None, this method will try
    to open a session using credentials found in accessKeys.csv

    Returns
    -------
    A list of dicts containing the metadata for the atlas.
    This is the result of running json.load on section_data_sets.json
    """

    tmp_dir = _get_tmp_dir()

    file_name = os.path.join(tmp_dir, 'section_data_sets.json')

    if session is None:
        session = aws_utils.get_boto3_session()

    bucket_name = 'allen-mouse-brain-atlas'

    s3 = session.client('s3')
    # get the md5sum of the section_data_sets.json file
    # to determine if the file must be downloaded
    obj_list = s3.list_objects(Bucket=bucket_name,
                               Prefix='section_data_sets.json')['Contents']
    if len(obj_list) != 1:
        msg = '\nquerying bucket for section_data_sets.json '
        msg += 'returned %d results\n' % len(obj_list)
        raise RuntimeError(msg)

    must_download = False
    target_md5 = obj_list[0]['ETag'].replace('"','')
    if not os.path.exists(file_name):
        must_download = True
    else:
        if not os.path.isfile(file_name):
            raise RuntimeError('\n%s\nexists but is not a file' % file_name)

        if not _compare_md5(file_name, target_md5):
            must_download = True

    if must_download:
        print('downloading section_data_sets.json')
        s3.download_file(Bucket=bucket_name,
                         Key='section_data_sets.json',
                         Filename=file_name)

        if not _compare_md5(file_name, target_md5):
            msg = '\nDownloaded section_data_sets.json; '
            msg += 'md5 checksum != %s\n' % target_md5
            raise RuntimeError(msg)

    with open(file_name, 'rb') as in_file:
        metadata = json.load(in_file)

    return metadata
