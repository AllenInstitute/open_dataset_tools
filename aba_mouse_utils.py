import os
import PIL.Image
import tempfile
import hashlib
import json
import copy
import warnings
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
    Check whether or not aws_key needs to be downloaded to keep
    local_filename up-to-date.

    Parameters
    ----------
    aws_key is the Key of the file in S3

    local_filename is the name of the local corresponding to aws_key

    session is a boto3 session

    bucket_name is the name of the S3 bucket where the file resides
    (Default: 'allen-mouse-brain-atlas')

    Returns
    -------
    A boolean that is True if the file needs to be downloaded from S3

    A string containing the md5checksum of the file
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

    Parameters
    ----------
    aws_key is the Key of the file in S3

    local_filename is the name of the local corresponding to aws_key

    session is a boto3 session

    bucket_name is the name of the S3 bucket where the file resides
    (Default: 'allen-mouse-brain-atlas')

    Returns
    -------
    None; just download the file to the specified local_filename
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

        Parameters
        ----------
        section_id is an int indicating which section_data_set to
        load

        session is a boto3 session. If None, will open a session
        using the credentials found in accessKeys.csv in the
        same directory as metadata_utils.py. (Default: None)

        tmp_dir is the directory in which to store downloaded *.json
        files. If None, will use the tmp/ sub-directory of the
        directory containing metadata_utils.py. (Default:None)
        """
        self.tmp_dir=tmp_dir
        self.section_id = section_id
        if session is None:
            session = aws_utils.get_boto3_session()
        self.session = session
        self.metadata = get_section_metadata(section_id, session=session,
                                             tmp_dir=self.tmp_dir)

        # remove section images and construct dicts keyed on
        # tissue_index and sub_image_id
        tmp_section_images = self.metadata.pop('section_images')

        self.tissue_index_to_section_img = {}
        self.subimg_to_tissue_index = {}
        for img in tmp_section_images:
            tissue_index = img['section_number']
            assert tissue_index not in self.tissue_index_to_section_img
            self.tissue_index_to_section_img[tissue_index] = img
            subimg_id = img['id']
            assert subimg_id not in self.subimg_to_tissue_index
            self.subimg_to_tissue_index[subimg_id] = tissue_index

        self._tissue_indices = list(self.tissue_index_to_section_img.keys())
        self._tissue_indices.sort()
        self._subimg_ids = list(self.subimg_to_tissue_index.keys())
        self._subimg_ids.sort()

    @property
    def tissue_indices(self):
        """
        Return a sorted list of all of the tissue index values
        available for the section_data_set
        """
        return self._tissue_indices

    @property
    def sub_image_ids(self):
        """
        Return a sorted list of all the sub-image ID values
        for the section_data_set
        """
        return self._subimg_ids

    def image_metadata_from_tissue_index(self, tissue_index):
        """
        Return the metadata of the section_image associated with the
        specified tissue_index.

        Returns None if an invalid tissue_index is specified
        """
        if tissue_index not in self.tissue_index_to_section_img:
            warnings.warn("tissue_index %d does not "
                          "exist in section_data_set_%d" %
                          (tissue_index, self.section_id))
            return None

        return copy.deepcopy(self.tissue_index_to_section_img[tissue_index])

    def image_metadata_from_sub_image(self, sub_image):
        """
        Return the metadata of the section_image associated with the
        specified subimage ID

        Returns None if an invalid subimage ID is specified
        """
        if sub_image not in self.subimg_to_tissue_index:
            warnings.warn("sub_image %d does not exist "
                          "in section_data_set_%d" %
                          (sub_image, self.section_id))

            return None

        tissue_index = self.subimg_to_tissue_index[sub_image]
        return self.image_metadata_from_tissue_index(tissue_index)

    def _download_img(self, tissue_index, downsample, local_filename, clobber=False):
        """
        Download the TIFF file specified by fname and downsample

        Parameters
        ----------
        tissue_index is the tissue index of the sub-image whose TIFF file
        we are to download

        downsample is an integer denoting the downsampling tier to download

        local_filename is the file name where the TIFF file will be saved

        clobber is a boolean. If True, overwrite pre-existing local_filename.
        Otherwise, throw a warning and exit if local_filename already exists

        Returns
        -------
        True if the TIFF file was successfully downloaded to local_filename;
        False if not.
        """

        if os.path.exists(local_filename):
            if not os.path.isfile(local_filename):
                warnings.warn('%s already exists but is not a file' % local_filename)
                return False
            if not clobber:
                warnings.warn("%s already exists; re-run with "
                              "clobber=True to overwrite" % local_filename)
                return False

        img_metadata = self.image_metadata_from_tissue_index(tissue_index)
        fname = img_metadata['image_file_name']

        downsample_key = 'downsample_%d' % downsample
        if downsample_key not in img_metadata['downsampling'].keys():
            warnings.warn("%d is not a valid downsampling tier for %s"
                          % (downsample, fname))
            return False
        aws_key = 'section_data_set_%d/%s/%s' % (self.section_id, downsample_key, fname)

        # Download the TIFF into a temporary location
        # then use PIL to crop the image to only include
        # the specified section of brain.

        tmp_filename = tempfile.mkstemp(dir=self.tmp_dir,
                                        prefix='tmp_before_crop_',
                                        suffix='.tiff')[1]

        s3 = self.session.client('s3')
        s3.download_file(Bucket='allen-mouse-brain-atlas',
                         Key=aws_key,
                         Filename=tmp_filename)

        img = PIL.Image.open(tmp_filename)
        tier_metadata = img_metadata['downsampling'][downsample_key]
        x0 = tier_metadata['x']
        y0 = tier_metadata['y']
        x1 = x0 + tier_metadata['width']
        y1 = y0 + tier_metadata['height']
        cropped_img = img.crop((x0, y0, x1, y1))
        cropped_img.save(local_filename)

        if os.path.exists(tmp_filename):
            os.unlink(tmp_filename)

        return True

    def download_image_from_tissue_index(self, tissue_index, downsample,
                                         local_filename, clobber=False):
        """
        Download a TIFF file specified by its tissue_index and downsampling
        tier.

        Parameters
        ----------
        tissue_index is an integer corressponding to the
        tissue_index/section_number of the TIFF to be downloaded

        downsample is an integer denoting the downsampling
        tier of the TIFF to be downloaded

        local_filename is the file name where the downloaded
        TIFF file should be saved

        clobber is a boolean. If True, overwrite pre-existing
        local_filename. If False, raise a warning and exit in
        the case where local_filename already exists

        Returns
        -------
        True if the TIFF was successfully downloaded to local_filename;
        False if not
        """
        if tissue_index not in self.tissue_index_to_section_img:
            warnings.warn("tissue_index %d does not exist in "
                          "section_data_set_%d" %
                          (tissue_index, self.section_id))
            return False
        return self._download_img(tissue_index, downsample, local_filename, clobber=clobber)

    def download_image_from_sub_image(self, sub_image, downsample,
                                      local_filename, clobber=False):
        """
        Download a TIFF file specified by its sub-image ID and downsampling
        tier.

        Parameters
        ----------
        sub_image is an integer corressponding to the sub-image ID
        of the TIFF to be downloaded

        downsample is an integer denoting the downsampling tier of
        the TIFF to be downloaded

        local_filename is the file name where the downloaded TIFF
        file should be saved

        clobber is a boolean. If True, overwrite pre-existing
        local_filename. If False, raise a warning and exit in the
        case where local_filename already exists

        Returns
        -------
        True if the TIFF was successfully downloaded to local_filename;
        False if not
        """
        if sub_image not in self.subimg_to_tissue_index:
            warnings.warn("sub_image %d does not exist "
                          "in section_data_set_%d" %
                          (sub_image, self.section_id))
            return False
        tissue_index = self.subimg_to_tissue_index[sub_image]
        return self.download_image_from_tissue_index(tissue_index, downsample,
                                                     local_filename, clobber=clobber)
