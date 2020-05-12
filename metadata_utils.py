import os
import hashlib
import json
import copy
import aws_utils


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

def _get_aws_md5(fname, session, bucket_name='allen-mouse-brain-atlas'):
    """
    Get and return the md5 checksum of a file in AWS
    """
    s3 = session.client('s3')
    # get the md5sum of the section_data_sets.json file
    # to determine if the file must be downloaded
    obj_list = s3.list_objects(Bucket=bucket_name,
                               Prefix=fname)['Contents']
    if len(obj_list) != 1:
        msg = '\nquerying bucket for %s ' % fname
        msg += 'returned %d results\n' % len(obj_list)
        raise RuntimeError(msg)

    return obj_list[0]['ETag'].replace('"','')


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


def _need_to_download(aws_key, local_filename, session,
                      bucket_name='allen-mouse-brain-atlas'):
    """
    Return a boolean indicating whether or not aws_key
    needs to be downloaded to keep local_filename up_to_date
    """
    target_md5 = _get_aws_md5(aws_key, session, bucket_name=bucket_name)
    must_download = False
    if not os.path.exists(local_filename):
        must_download = True
    else:
        if not os.path.isfile(local_filename):
            raise RuntimeError('\n%s\nexists but is not a file' % local_filename)

        if not _compare_md5(local_filename, target_md5):
            must_download = True
    return must_download, target_md5


def _get_aws_file(aws_key, local_filename, session,
                  bucket_name='allen-mouse-brain-atlas'):

    """
    Download the AWS file specified by bucket_name:aws_key to
    local_filename, but only if necessary
    """
    (must_download,
         target_md5) = _need_to_download(aws_key, local_filename, session,
                                         bucket_name=bucket_name)

    if must_download:
        print('downloading %s' % aws_key)
        s3 = session.client('s3')
        s3.download_file(Bucket=bucket_name,
                         Key=aws_key,
                         Filename=local_filename)

        if not _compare_md5(local_filename, target_md5):
            msg = '\nDownloaded section_data_sets.json; '
            msg += 'md5 checksum != %s\n' % target_md5
            raise RuntimeError(msg)

    return None


def get_atlas_metadata(session=None, tmp_dir=None):
    """
    Load the metadata for the entire atlas into memory.
    If you have not already downloaded this file, it will
    be downloaded into the tmp/ sub-directory of the directory
    containing metadata_utils.py

    Parameters
    ----------
    session is a boto3.Session. If None, this method will try
    to open a session using credentials found in accessKeys.csv

    tmp_dir is the directory to which temporary data should
    be downloaded. If None, then the data will be downloaded
    tmp/ in the directory where metadata_utils.py is
    (Default: None).

    Returns
    -------
    A list of dicts containing the metadata for the atlas.
    This is the result of running json.load on section_data_sets.json
    """

    if tmp_dir is None:
        tmp_dir = _get_tmp_dir()

    file_name = os.path.join(tmp_dir, 'section_data_sets.json')

    if session is None:
        session = aws_utils.get_boto3_session()

    bucket_name = 'allen-mouse-brain-atlas'
    _get_aws_file('section_data_sets.json', file_name, session,
                  bucket_name=bucket_name)

    with open(file_name, 'rb') as in_file:
        metadata = json.load(in_file)

    return metadata


def get_section_metadata(section_id, session=None, tmp_dir=None):
    """
    Get the dict representing the metadata for a specific image series.

    Parameters
    ----------
    section_id is an integer representing the section whose metadata should be
    loaded

    session is a boto3.Session. If None, this method will try to create one
    looking for credentialsi n accessKeys.csv

    tmp_dir is the directory to which temporary data should
    be downloaded. If None, then the data will be downloaded
    tmp/ in the directory where metadata_utils.py is
    (Default: None).

    Returns
    -------
    A dict containing the metadata for the specified Session.

    Note: this method will download the json file containing the metadata
    into the tmp/ sub directory of the directory containing metadata_utils.py
    """
    if tmp_dir is None:
        tmp_dir = _get_tmp_dir()

    if session is None:
        session = aws_utils.get_boto3_session()

    file_name = os.path.join(tmp_dir,
                            'section_data_set_%d_metadata.json' % section_id)

    aws_key = 'section_data_set_%d/section_data_set.json' % section_id

    _get_aws_file(aws_key, file_name, session,
                  bucket_name='allen-mouse-brain-atlas')

    with open(file_name, 'rb') as in_file:
        metadata = json.load(in_file)

    return metadata


class SectionDataSet(object):

    def __init__(self, section_id, session=None, tmp_dir=None):
        """
        Load and store the metadata for the section_data_set specified
        by section_id. Use the boto3 session provided as a kwarg, or
        open a session using credentials found in accessKeys.csv
        """
        self.tmp_dir=tmp_dir
        self.section_id = section_id
        if session is None:
            session = aws_utils.get_boto3_session()
        self.session = session
        self.metadata = get_section_metadata(section_id, session=session,
                                             tmp_dir=self.tmp_dir)

        # remove section images and construct a dict keyed on image name
        tmp_section_images = self.metadata.pop('section_images')

        self.section_images = {}
        self.tissue_index_to_img = {}
        self.subimg_to_img = {}
        for img in tmp_section_images:
            fname = img['image_file_name']
            self.section_images[fname] = img
            self.tissue_index_to_img[img['section_number']] = fname
            self.subimg_to_img[img['id']] = fname

    def image_metadata_from_tissue_index(self, tissue_index):
        """
        Return the metadata of the section_image associated with the
        specified tissue_index.

        Returns None if an invalid tissue_index is specified
        """
        if tissue_index not in self.tissue_index_to_img:
            print("tissue_index %d does not exist in section_data_set_%d" %
                   (tissue_index, self.section_id))
            return None

        fname = self.tissue_index_to_img[tissue_index]
        return copy.deepcopy(self.section_images[fname])

    def image_metadata_from_sub_image(self, sub_image):
        """
        Return the metadata of the section_image associated with the
        specified subimage ID

        Returns None if an invalid subimage ID is specified
        """
        if sub_image not in self.subimg_to_img:
            print("sub_image %d does not exst in section_data_set_%d" %
                  (sub_image, self.section_id))

            return None

        fname = self.subimg_to_img
        return copy.deepcopy(self.section_images[fname])

    def _download_img(self, fname, downsample, local_filename, clobber=False):

        if os.path.exists(local_filename):
            if not os.path.isfile(local_filename):
                print('%s already exists but is not a file' % local_filename)
                return False
            if not clobber:
                print('%s already exists; re-run with clobber=True to overwrite')
                return False

        downsample_key = 'downsample_%d' % downsample
        if downsample_key not in self.section_images[fname].keys():
            print("%d is not a valid downsampling tier for %s" % (downsample, fname))
        aws_key = 'section_data_set_%d/%s/%s' % (self.section_id, downsample_key, fname)

        s3 = self.session.client('s3')
        s3.download_file(Bucket='allen-mouse-brain-atlas',
                         Key=aws_key,
                         Filename=local_filename)

        return True

    def download_image_from_tissue_index(self, tissue_index, downsample,
                                         local_filename, clobber=False):

        if tissue_index not in self.tissue_index_to_img:
            print("tissue_index %d does not exist in section_data_set_%d" %
                   (tissue_index, self.section_id))
            return False
        fname = self.tissue_index_to_img[tissue_index]
        return self._download_img(fname, downsample, local_filename, clobber=clobber)

    def download_image_from_sub_image(self, sub_image, downsample,
                                      local_filename, clobber=False):

        if sub_image not in self.subimg_to_img:
            print("sub_image %d does not exst in section_data_set_%d" %
                  (sub_image, self.section_id))
            return False
        fname = self.sub_img_to_img[sub_image]
        return self._download_img(fname, downsample, local_filename, clobber=clobber)
