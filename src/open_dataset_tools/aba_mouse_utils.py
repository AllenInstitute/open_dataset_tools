from typing import List, Tuple, Union
from pathlib import Path
import tempfile
import hashlib
import json
import copy
import warnings

from PIL import Image
from PIL import ImageFile

from open_dataset_tools.aws_utils import get_public_boto3_client


ImageFile.LOAD_TRUNCATED_IMAGES = True


def _get_aws_md5(
    fname: str, s3_client, bucket_name='allen-mouse-brain-atlas'
) -> str:
    """
    Get and return the md5 checksum (str) of a file in AWS
    """
    # get the md5sum of the section_data_sets.json file
    # to determine if the file must be downloaded
    obj_list = s3_client.list_objects(Bucket=bucket_name,
                               Prefix=fname)['Contents']
    if len(obj_list) != 1:
        msg = '\nquerying bucket for %s ' % fname
        msg += 'returned %d results\n' % len(obj_list)
        raise RuntimeError(msg)

    return obj_list[0]['ETag'].replace('"','')


def _compare_md5(fname: Path, target: str) -> bool:
    """
    Compare the md5 checksum of the file specified by fname to the
    string specified by target. Return boolean result indicating if
    they are equal.
    """
    md5_obj = hashlib.md5()
    with open(fname, 'rb') as in_file:
        for line in in_file:
            md5_obj.update(line)
    return md5_obj.hexdigest() == target


def _need_to_download(
    aws_key: str, local_filename: Path, s3_client,
    bucket_name='allen-mouse-brain-atlas'
) -> Tuple[bool, str]:
    """
    Check whether or not aws_key needs to be downloaded to keep
    local_filename up-to-date.

    Parameters
    ----------
    aws_key is the Key of the file in S3

    local_filename is the name of the local corresponding to aws_key

    s3_client is a boto3 client for the AWS S3 service

    bucket_name is the name of the S3 bucket where the file resides
    (Default: 'allen-mouse-brain-atlas')

    Returns
    -------
    A boolean that is True if the file needs to be downloaded from S3

    A string containing the md5checksum of the file
    """
    target_md5 = _get_aws_md5(aws_key, s3_client, bucket_name=bucket_name)
    must_download = False
    if not local_filename.exists():
        must_download = True
    else:
        if not local_filename.is_file():
            raise RuntimeError(
                '\n%s\nexists but is not a file' % local_filename
            )

        if not _compare_md5(local_filename, target_md5):
            must_download = True
    return must_download, target_md5


def _get_aws_file(aws_key, local_filename: Path, s3_client,
                  bucket_name='allen-mouse-brain-atlas'):
    """
    Download the AWS file specified by bucket_name:aws_key to
    local_filename, but only if necessary

    Parameters
    ----------
    aws_key is the Key of the file in S3

    local_filename is the name of the local corresponding to aws_key

    s3_client is a boto3 client for the AWS S3 service

    bucket_name is the name of the S3 bucket where the file resides
    (Default: 'allen-mouse-brain-atlas')

    Returns
    -------
    None; just download the file to the specified local_filename
    """
    (must_download, target_md5) = _need_to_download(
        aws_key, local_filename, s3_client,bucket_name=bucket_name
    )

    if must_download:
        print('Downloading %s' % aws_key)
        s3_client.download_file(
            Bucket=bucket_name,
            Key=aws_key,
            Filename=str(local_filename)
        )

        if not _compare_md5(local_filename, target_md5):
            msg = '\nDownloaded section_data_sets.json; '
            msg += 'md5 checksum != %s\n' % target_md5
            raise RuntimeError(msg)

    return None


def download_s3_metadata_file(
    download_directory: Union[str, Path],
    downloaded_local_fname: str,
    metadata_s3_key: str,
    s3_client = None,
    bucket_name: str = "allen-mouse-brain-atlas"
) -> Union[dict, List[dict]]:
    """Download and parse a metadata *.json file for the Allen Mouse Brain
    Atlas dataset.

    Parameters
    ----------
    download_directory : Union[str, Path]
        The desired local directory path to save downloaded metadata. If the
        provided directory does not exist, it and any necessary parent
        directories will be created automatically.
    downloaded_local_fname : str
        The file name that the downloaded metadata should have.
    metadata_s3_key : str
        The S3 Key used to select which file to downloaded.
    s3_client :
        A boto3.Client of the S3 variety. If None, an s3 client with
        anonymous credentials will be automatically created.
    bucket_name : str, optional
        The name of the bucket to download the metadata file from,
        by default "allen-mouse-brain-atlas"

    Returns
    -------
    Union[dict, List[dict]]
        The parsed contents of a *.json file. Can be a list of dicts or
        just a dict.
    """
    if type(download_directory) is str:
        download_directory = Path(download_directory).resolve()

    if not download_directory.exists():
        download_directory.mkdir(parents=True, exist_ok=True)

    if s3_client is None:
        s3_client = get_public_boto3_client()

    local_metadata_path = download_directory / downloaded_local_fname

    _get_aws_file(
        aws_key=metadata_s3_key,
        local_filename=local_metadata_path,
        s3_client=s3_client,
        bucket_name=bucket_name
    )

    with local_metadata_path.open('rb') as f:
        metadata = json.load(f)

    return metadata


def get_atlas_metadata(
    download_directory: Union[str, Path],
    s3_client=None,
    bucket_name: str = 'allen-mouse-brain-atlas'
) -> List[dict]:
    """
    Load the metadata for the entire atlas into memory.
    If you have not already downloaded this file, it will
    be downloaded to the specified `download_directory`

    Parameters
    ----------
    s3_client
        A boto3.Client of the S3 variety. If None, this function will
        try to create an s3 client with anonymous credentials which is
        sufficient to access public AWS services
    download_directory : Union[str, Path]
        The desired local directory path to save downloaded metadata. If the
        provided directory does not exist, it and any necessary parent
        directories will be created automatically.
    bucket_name : str
        The name of the S3 bucket to download metadata from.

    Returns
    -------
    A list of dicts containing the metadata for the atlas.
    This is the result of running json.load on section_data_sets.json
    """

    metadata = download_s3_metadata_file(
        download_directory=download_directory,
        metadata_s3_key="section_data_sets.json",
        downloaded_local_fname="section_data_sets.json",
        bucket_name=bucket_name
    )

    return metadata


def get_section_metadata(
        section_id: int,
        download_directory: Union[str, Path],
        s3_client=None,
        bucket_name: str = 'allen-mouse-brain-atlas'
    ) -> dict:
    """
    Get the dict representing the metadata for a specific image series.

    Parameters
    ----------
    section_id : int
        An integer representing the section whose metadata should be loaded
    s3_client
        A boto3.Client of the S3 variety. If None, this function will
        try to create an s3 client with anonymous credentials which is
        sufficient to access public AWS services
    download_directory : Union[str, Path]
        The desired local directory path to save downloaded metadata. If the
        provided directory does not exist, it and any necessary parent
        directories will be created automatically.
    bucket_name : str
        The name of the S3 bucket to download metadata from.

    Returns
    -------
    A dict containing the metadata for the specified section_id.
    """

    metadata_s3_key = f"section_data_set_{section_id}/section_data_set.json"
    local_fname = f"section_data_set_{section_id}_metadata.json"

    metadata = download_s3_metadata_file(
        download_directory=download_directory,
        metadata_s3_key=metadata_s3_key,
        downloaded_local_fname=local_fname,
        bucket_name=bucket_name
    )

    return metadata


class SectionDataSet(object):

    def __init__(
        self,
        section_id: int,
        download_directory: Union[str, Path],
        s3_client=None
    ):
        """
        Load and store the metadata for the section_data_set specified
        by section_id. Use the boto3 s3_client provided as a kwarg.

        Parameters
        ----------
        section_id :
            An int indicating which section_data_set to load
        download_directory : Union[str, Path]
            The desired local directory path to save downloaded metadata. If
            the provided directory does not exist, it and any necessary parent
            directories will be created automatically.
        s3_client :
            A boto3.Client of the S3 variety. If None, an s3 client with
            anonymous credentials will be automatically created.
        """

        if type(download_directory) is str:
            download_directory = Path(download_directory).resolve()

        if not download_directory.exists():
            download_directory.mkdir(parents=True, exist_ok=True)

        if s3_client is None:
            s3_client = get_public_boto3_client()

        self.download_dir = download_directory
        self.section_id = section_id
        self.s3_client = s3_client
        self.metadata = get_section_metadata(
            section_id=section_id,
            download_directory=download_directory
        )

        # remove section images and construct dicts keyed on
        # tissue_index and sub_image_id
        tmp_section_images = self.metadata.pop('section_images')

        self.tissue_index_to_section_img = {}
        self.subimg_to_tissue_index = {}
        self.tissue_index_to_subimg = {}
        for img in tmp_section_images:
            tissue_index = img['section_number']
            assert tissue_index not in self.tissue_index_to_section_img
            self.tissue_index_to_section_img[tissue_index] = img
            subimg_id = img['id']
            assert subimg_id not in self.subimg_to_tissue_index
            self.subimg_to_tissue_index[subimg_id] = tissue_index
            assert tissue_index not in self.tissue_index_to_subimg
            self.tissue_index_to_subimg[tissue_index] = subimg_id

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

    def _download_img(
        self, tissue_index: int, downsample: int,
        local_savepath: Path, clobber: bool = False
    ):
        """
        Download the TIFF file specified by fname and downsample

        Parameters
        ----------
        tissue_index is the tissue index of the sub-image whose TIFF file
        we are to download

        downsample is an integer denoting the downsampling tier to download

        local_savepath is the file path where the TIFF file will be saved

        clobber is a boolean. If True, overwrite pre-existing local_savepath.
        Otherwise, throw a warning and exit if local_savepath already exists

        Returns
        -------
        True if the TIFF file was successfully downloaded to local_savepath;
        False if not.
        """

        if local_savepath.exists():
            if not local_savepath.is_file():
                warnings.warn(
                    '%s already exists but is not a file' % local_savepath
                )
                return False
            if not clobber:
                warnings.warn("%s already exists; re-run with "
                              "clobber=True to overwrite" % local_savepath)
                return False

        img_metadata = self.image_metadata_from_tissue_index(tissue_index)
        fname = img_metadata['image_file_name']

        downsample_key = 'downsample_%d' % downsample
        if downsample_key not in img_metadata['downsampling'].keys():
            warnings.warn("%d is not a valid downsampling tier for %s"
                          % (downsample, fname))
            return False
        aws_key = 'section_data_set_%d/%s/%s' % (
            self.section_id, downsample_key, fname
        )

        # Download the TIFF into a temporary location
        # then use PIL to crop the image to only include
        # the specified section of brain.

        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=self.download_dir,
            prefix="tmp_before_crop_",
            suffix=".tiff"
        ) as f:

            self.s3_client.download_fileobj(
                Bucket='allen-mouse-brain-atlas',
                Key=aws_key,
                Fileobj=f
            )

            tier_metadata = img_metadata['downsampling'][downsample_key]
            x0 = tier_metadata['x']
            y0 = tier_metadata['y']
            x1 = x0 + tier_metadata['width']
            y1 = y0 + tier_metadata['height']

            with Image.open(Path(f.name).resolve(), "r") as img:
                cropped_img = img.crop((x0, y0, x1, y1))
                cropped_img.save(str(local_savepath))
                cropped_img.close()

        return True

    def download_image_from_tissue_index(
        self, tissue_index: int, downsample: int,
        local_savepath: Path, clobber: bool = False
    ):
        """
        Download a TIFF file specified by its tissue_index and downsampling
        tier.

        Parameters
        ----------
        tissue_index is an integer corressponding to the
        tissue_index/section_number of the TIFF to be downloaded

        downsample is an integer denoting the downsampling
        tier of the TIFF to be downloaded

        local_savepath is the file path where the downloaded
        TIFF file should be saved

        clobber is a boolean. If True, overwrite pre-existing
        local_savepath. If False, raise a warning and exit in
        the case where local_savepath already exists

        Returns
        -------
        True if the TIFF was successfully downloaded to local_savepath;
        False if not
        """
        if tissue_index not in self.tissue_index_to_section_img:
            warnings.warn("tissue_index %d does not exist in "
                          "section_data_set_%d" %
                          (tissue_index, self.section_id))
            return False
        return self._download_img(
            tissue_index, downsample, local_savepath, clobber=clobber
        )

    def download_image_from_sub_image(
        self, sub_image: int, downsample: int,
        local_savepath: str, clobber: bool = False
    ):
        """
        Download a TIFF file specified by its sub-image ID and downsampling
        tier.

        Parameters
        ----------
        sub_image is an integer corressponding to the sub-image ID
        of the TIFF to be downloaded

        downsample is an integer denoting the downsampling tier of
        the TIFF to be downloaded

        local_savepath is the file name where the downloaded TIFF
        file should be saved

        clobber is a boolean. If True, overwrite pre-existing
        local_savepath. If False, raise a warning and exit in the
        case where local_savepath already exists

        Returns
        -------
        True if the TIFF was successfully downloaded to local_savepath;
        False if not
        """
        if sub_image not in self.subimg_to_tissue_index:
            warnings.warn("sub_image %d does not exist "
                          "in section_data_set_%d" %
                          (sub_image, self.section_id))
            return False
        tissue_index = self.subimg_to_tissue_index[sub_image]
        return self.download_image_from_tissue_index(
            tissue_index, downsample, local_savepath, clobber=clobber
        )

    def section_url(self):
        """
        Return the URL for the brain-map.org viewer for this SectionDataSet
        """
        return "http://mouse.brain-map.org/experiment/show/{id}".format(id=self.section_id)

    def sub_image_url(self, sub_image_id: int) -> str:
        """
        Return URL for a high quality image of a specific sub-image,
        specified by sub_image_id
        """
        base = "http://mouse.brain-map.org/experiment/siv?id={sect}&imageId={img}&initImage=ish"
        return base.format(sect=self.section_id, img=sub_image_id)

    def tissue_index_url(self, tissue_index: int) -> str:
        """
        Return URL for a high quality image of a specific sub-image,
        specified by tissued_index
        """
        sub_img = self.tissue_index_to_subimg[tissue_index]
        return self.sub_image_url(sub_img)
