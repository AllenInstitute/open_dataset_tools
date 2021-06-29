import io
import warnings
from typing import Optional
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.image as mp_img
from PIL import Image

from open_dataset_tools.aws_utils import get_public_boto3_client


def load_s3_json_as_dataframe(client, bucket: str, key: str) -> pd.DataFrame:
    """Given a boto3 S3 client, an S3 bucket name, and a key for a
    JSON file to be downloaded, download it and parse it as a pandas
    DataFrame.

    Parameters
    ----------
    client : Boto3 S3 Client
        A boto3 s3 Client object
    bucket : str
        The name of the bucket to download JSON file from
    key : str
        The S3 key for accessing the JSON file

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame
    """
    obj = client.get_object(Bucket=bucket, Key=key)["Body"]
    json_data = io.BytesIO(obj.read())
    return pd.read_json(json_data)


def get_donor_metadata(client) -> pd.DataFrame:
    bucket = "allen-ivy-glioblastoma-atlas"
    key = "donor_metadata.json"
    return load_s3_json_as_dataframe(client, bucket, key)


def get_specimen_metadata(client) -> pd.DataFrame:
    bucket = "allen-ivy-glioblastoma-atlas"
    key = "specimen_metadata.json"
    return load_s3_json_as_dataframe(client, bucket, key)


def get_section_metadata(client) -> pd.DataFrame:
    bucket = "allen-ivy-glioblastoma-atlas"
    key = "section_metadata.json"
    return load_s3_json_as_dataframe(client, bucket, key)


class ImagePromise(object):
    """The ImagePromise class is intended to defer loading IVY GAP image
    data until absolutely necessary, because the images take up a potentially
    huge amount of memory.

    This class is instantiated with image width/height in pixels as well as an
    s3 object url string which specifies a specific image:
    s3://allen-ivy-glioblastoma-atlas/{specific_image_resource_key}

    Optionally, a local save directory can also be provided that contains
    the specific image to have the class load images from local disk
    instead of downloading from S3.

    When the load() method is called, if only an s3_obj_url was provided
    then the image repreasented by the ImagePromise will be downloaded from
    S3. If both s3_obj_url and local_save_directory were provided, then
    the image will be loaded from local disk.
    """

    def __init__(
        self,
        s3_obj_url: str,
        image_width: int,
        image_height: int,
        local_save_directory: Optional[Path] = None,
        s3_client=None,
        verbose: bool = False
    ):
        # Absolutely necessary parameters
        self._s3_obj_url = s3_obj_url
        self._image_width = image_width
        self._image_height = image_height

        # Need either local_save_directory or s3_client
        # Give a warning if both are provided and prefer local_save_directory
        if local_save_directory and s3_client:
            warnings.warn(
                "Both an s3_client as well as local_save_directory parameter "
                "were provided, the s3_client parameter will be ignored!"
            )

        if local_save_directory is None and s3_client is None:
            raise RuntimeError(
                "Must provide either an s3_client parameter or a "
                "local_save_directory parameter!"
            )
        self._s3_client = s3_client
        self._local_save_dir = local_save_directory

        # Completely optional
        self._verbose = verbose

    @property
    def num_pixels(self) -> int:
        return self._image_width * self._image_height

    def load(self) -> Image:

        rel_path = self._s3_obj_url.lstrip("s3://allen-ivy-glioblastoma-atlas/")

        if self._local_save_dir is not None:
            file_object = (self._local_save_dir / rel_path).resolve()
            if self._verbose:
                print(f"Loading image from: {str(file_object)}")
        else:
            file_object = io.BytesIO()
            if self._verbose:
                print(f"Downloading image from: {self._s3_obj_url}")
            img_obj = self._s3_client.download_fileobj(
                Bucket="allen-ivy-glioblastoma-atlas",
                Key=rel_path,
                Fileobj=file_object
            )

        Image.MAX_IMAGE_PIXELS = self.num_pixels
        with Image.open(file_object, "r") as img:
            img.load()

        return img


def section_image_loader(
    section_meta_table: pd.DataFrame,
    section_data_set_id: int,
    local_save_directory: Optional[Path] = None,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Given a section metadata DataFrame and a specific `section_data_set_id`
    return a DataFrame containing image data and metadata associated with
    the requested `section_data_set_id`.

    Parameters
    ----------
    section_meta_table : pd.DataFrame
        A section metadata table (can be obtained from `get_section_metadata()`
        or `local_section_metadata_loader`)
    section_data_set_id : int
        A unique identifier for a section entry in the section_meta_table
    local_save_directory: Optional[Path]
        A Python pathlib.Path object pointing to a user local directory
        where IVY GAP images have already been downloaded. If this parameter
        is left as None, then images will be downloaded from AWS S3.
    verbose:
        Whether this function should print updates on what it is downloading
    
    Returns
    -------
    pd.DataFrame
        A section image table that contains metadata about images associated
        with a section as well as "ImagePromise" objects which will
        yield PIL "Image" object instances when ImagePromise.load() is called.
    """

    try:
        sub_images = section_meta_table[
            section_meta_table["section_data_set_id"] == section_data_set_id
        ]["sub_images"].iloc[0]

    except IndexError as e:
        e_msg = (
            f"Could not find the `section_data_set_id` specified "
            f"({section_data_set_id}) in the `section_meta_table` provided!"
        )
        raise RuntimeError(e_msg) from e

    if local_save_directory is None:
        s3_client = get_public_boto3_client()

    loaded_sub_images = []
    for img_dict in sub_images:
        new_img_dict = {**img_dict}

        for k, v in new_img_dict["s3_data"].items():

            # If user hasn't provided a local_save_directory then assume
            # we need to download from S3
            if local_save_directory is None:
                image_promise = ImagePromise(
                    s3_obj_url=v,
                    image_width=img_dict["width"],
                    image_height=img_dict["height"],
                    s3_client=s3_client,
                    verbose=verbose
                )
            else:
                image_promise = ImagePromise(
                    s3_obj_url=v,
                    image_width=img_dict["width"],
                    image_height=img_dict["height"],
                    local_save_directory=local_save_directory,
                    verbose=verbose                    
                )

            new_img_dict[k] = image_promise

        del new_img_dict["s3_data"]
        loaded_sub_images.append(new_img_dict)

    return pd.DataFrame(loaded_sub_images)


def local_section_metadata_loader(
    local_save_directory: Path,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Download and save the section metadata file to a local save directory.
    If local_save_directory contains a previously downloaded section metadata file,
    that file will be loaded in lieu of a download.

    Parameters
    ----------
    local_save_directory : Path
        A local path where the Ivy GAP dataset should be downloaded
    verbose : bool
        Whether detailed information about what files are being
        downloaded or loaded should be shown.
    
    Returns
    -------
    pd.DataFrame
        A section metadata table
    """

    s3_client = get_public_boto3_client()
    
    if not local_save_directory.exists():
        local_save_directory.mkdir(parents=True, exist_ok=True)
    
    section_metadata_cache_loc = local_save_directory / "section_metadata.json"
    if not section_metadata_cache_loc.exists():
        # Download and cache section_metadata table since it is the
        # only metadata of appreciable size (~56MB)
        if verbose:
            print(
                f"Downloading section_metadata.json to "
                f"{str(section_metadata_cache_loc)}\n"
            )
        section_metadata = get_section_metadata(s3_client)
        section_metadata.to_json(
            section_metadata_cache_loc,
            orient="records",
            indent=4
        )
    else:
        if verbose:
            print(
                f"Loading section_metadata.json from "
                f"{str(section_metadata_cache_loc)}\n"
            )
        section_metadata = pd.read_json(section_metadata_cache_loc)
    
    return section_metadata


def section_image_downloader(
    local_save_directory: Path,
    section_data_set_id: int,
    verbose: bool = False):
    """
    Given a `local_save_directory` and a specific `section_data_set_id`
    download image data associated with the requested `section_data_set_id`
    and save it to the `local_save_directory`

    Parameters
    ----------
    local_save_directory : Path
        Path to the desired local directory where downloaded images should
        be saved
    section_data_set_id : int
        A unique identifier for a section entry in the section_meta_table
    verbose:
        Whether this function should print updates on what it is downloading
    """

    s3_client = get_public_boto3_client()
    
    section_metadata = local_section_metadata_loader(
        local_save_directory, verbose=verbose
    )
    
    try:
        sub_images = section_metadata[
            section_metadata["section_data_set_id"] == section_data_set_id
        ]["sub_images"].iloc[0]
    except IndexError as e:
        e_msg = (
            f"Could not find the `section_data_set_id` specified "
            f"({section_data_set_id}) in the `section_meta_table` provided!"
        )
        raise RuntimeError(e_msg) from e

    for img_dict in sub_images:
        
        for k, v in img_dict["s3_data"].items():
            rel_path = v.lstrip("s3://allen-ivy-glioblastoma-atlas/")
            local_save_path = local_save_directory / rel_path
            
            # Create parent directories if they don't exist
            local_save_path.parent.mkdir(parents=True, exist_ok=True)

            if verbose:
                print(f"Saving image from {v} to {str(local_save_path)}\n")
            
            with local_save_path.open('wb') as fp:
                s3_client.download_fileobj(
                    Bucket="allen-ivy-glioblastoma-atlas",
                    Key=rel_path,
                    Fileobj=fp
                )
