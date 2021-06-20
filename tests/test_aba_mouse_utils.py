import contextlib
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import warnings
from pathlib import Path

from PIL import Image

import open_dataset_tools.aba_mouse_utils as mouse_utils
from open_dataset_tools.aws_utils import get_public_boto3_client


@contextlib.contextmanager
def make_tmp_dir(auto_delete: bool = True):
    tmp_dir_base = Path(__file__).resolve().parent / 'test_tmp'
    tmp_dir_base.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp(dir=tmp_dir_base))

    try:
        yield tmp_dir
    finally:
        if auto_delete:
            shutil.rmtree(tmp_dir)

class MetadataTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # regarding warnings filter, see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message='unclosed <ssl.SSLSocket')

        cls.s3_client = get_public_boto3_client()

    def test_atlas_metadata(self):
        """
        Test that we can download the global section_data_sets.json
        file. Compare its md5 checksum to a hard-coded value taken
        from a verified copy that was downloaded from S3 by hand.
        """
        with make_tmp_dir() as tmp_dir:
            for s3_client in (self.s3_client, None):
                metadata = mouse_utils.get_atlas_metadata(
                    s3_client=s3_client,
                    download_directory=tmp_dir
                )

                self.assertIsInstance(metadata, list)
                self.assertEqual(len(metadata), 26078)
                for obj in metadata:
                    self.assertIsInstance(obj, dict)

                fname = tmp_dir / 'section_data_sets.json'

                if not fname.exists():
                    raise RuntimeError("Failed to download section_data_sets.json")

                md5_obj = hashlib.md5()
                with fname.open('rb') as in_file:
                    for line in in_file:
                        md5_obj.update(line)
                checksum = md5_obj.hexdigest()
                self.assertEqual(checksum,
                                '2c974e2be3a30a4d923f47dd4a7fde72')

    def test_section_metadata(self):
        """
        Test downloading a specific section's metadata. Verify the file
        against a hard-coded md5 checksum
        """
        with make_tmp_dir() as tmp_dir:
            section_id = 99
            for s3_client in (self.s3_client, None):
                metadata = mouse_utils.get_section_metadata(
                    section_id=section_id,
                    download_directory=tmp_dir
                )

                self.assertIsInstance(metadata, dict)

                fname = tmp_dir / f'section_data_set_{section_id}_metadata.json'

                if not fname.exists():
                    raise RuntimeError("Failed to download %s" % fname)

                md5_obj = hashlib.md5()
                with fname.open('rb') as in_file:
                    for line in in_file:
                        md5_obj.update(line)
                checksum = md5_obj.hexdigest()
                self.assertEqual(checksum,
                                'e8eff384bb39cc981f93bad62e6fad02')

    def test_download_caching(self):
        """
        Test that the method to download metadata does not download it twice
        unnecessarily
        """
        with make_tmp_dir() as tmp_dir:
            section_id = 99
            fname = tmp_dir / f'section_data_set_{section_id}_metadata.json'
            aws_name = f'section_data_set_{section_id}/section_data_set.json'
            self.assertFalse(fname.exists())
            mouse_utils._get_aws_file(aws_name, fname, self.s3_client)
            self.assertTrue(fname.exists)
            fstats = os.stat(fname)
            t0 = fstats.st_mtime_ns  # get the time of last modificatio in nanosec
            time.sleep(2)  # wait so that, if redownloaded, st_mtime_ns would differ
            mouse_utils._get_aws_file(aws_name, fname, self.s3_client)
            fstats = os.stat(fname)
            t1 = fstats.st_mtime_ns
            self.assertEqual(t1, t0)


class SectionDataSetTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # regarding warnings filter, see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message='unclosed <ssl.SSLSocket')
        cls.s3_client = get_public_boto3_client()
        cls.example_section_id = 275693

    def test_metadata_from_tissue_index(self):
        """
        Try loading the metadata by tissue_index.
        Compare to a json dict of the expected result that was
        copied to tests/test_data/ by hand.
        """

        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client           
            )

            metadata = dataset.image_metadata_from_tissue_index(154)
            control_file = (
                Path(__file__).resolve().parent
                / 'test_data'
                / 'example_metadata_tissue_154.json'
            )
            with open(control_file, 'rb') as in_file:
                control_metadata = json.load(in_file)
            self.assertEqual(metadata, control_metadata)

            # try loading a bad value
            with self.assertWarns(UserWarning) as bad_tissue:
                metadata = dataset.image_metadata_from_tissue_index(999)
            self.assertIsNone(metadata)
            self.assertIn("tissue_index 999 does not exist",
                        bad_tissue.warning.args[0])

    def test_metadata_from_sub_image(self):
        """
        Try loading the metadata by sub_image_id.
        Compare to a json dict of the expected result that was
        copied to tests/test_data/ by hand.
        """

        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            metadata = dataset.image_metadata_from_sub_image(102000022)
            control_file = (
                Path(__file__).resolve().parent
                / 'test_data'
                / 'example_metadata_id_102000022.json'
            )
            with open(control_file, 'rb') as in_file:
                control_metadata = json.load(in_file)
            self.assertEqual(metadata, control_metadata)

            # try loading a bad value
            with self.assertWarns(UserWarning) as bad_tissue:
                metadata = dataset.image_metadata_from_sub_image(999)
            self.assertIsNone(metadata)
            self.assertIn("sub_image 999 does not exist",
                        bad_tissue.warning.args[0])

    def test_bad_tier_image_download(self):
        """
        Test behavior of SectionDataSet when you ask it to
        download images from tiers that do not exist
        """
        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            # verify what happens when you ask for a resolution
            # that does not exist
            tiff_name = tmp_dir / 'junk.tiff'
            with self.assertWarns(UserWarning) as bad_image:
                res = dataset.download_image_from_tissue_index(66,
                                                            0,
                                                            tiff_name)
            self.assertIs(res, False)
            self.assertFalse(tiff_name.exists())
            self.assertIn("0 is not a valid downsampling tier",
                        bad_image.warning.args[0])
            metadata = dataset.image_metadata_from_tissue_index(66)
            self.assertNotIn('downsample_0', metadata)

            with self.assertWarns(UserWarning) as bad_image:
                res = dataset.download_image_from_sub_image(102000016,
                                                            0,
                                                            tiff_name)
            self.assertIs(res, False)
            self.assertFalse(tiff_name.exists())
            self.assertIn("0 is not a valid downsampling tier",
                        bad_image.warning.args[0])
            metadata = dataset.image_metadata_from_sub_image(102000016)
            self.assertNotIn('downsample_0', metadata)

    def test_bad_identifier_image_download(self):
        """
        Test behavior of SectionDataSet when you ask it to
        download images from tissue_indexes/sub_images
        that do not exist
        """
        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            # verify what happens when you ask for a resolution
            # that does not exist
            tiff_name = tmp_dir / 'junk2.tiff'
            with self.assertWarns(UserWarning) as bad_image:
                res = dataset.download_image_from_tissue_index(999,
                                                            4,
                                                            tiff_name)
            self.assertIs(res, False)
            self.assertFalse(tiff_name.exists())
            self.assertIn("tissue_index 999 does not exist",
                        bad_image.warning.args[0])

            with self.assertWarns(UserWarning) as bad_image:
                res = dataset.download_image_from_sub_image(999,
                                                            4,
                                                            tiff_name)
            self.assertIs(res, False)
            self.assertFalse(tiff_name.exists())
            self.assertIn("sub_image 999 does not exist",
                        bad_image.warning.args[0])

    def test_good_tier_image_download(self):
        """
        Test behavior of SectionDataSet when you ask it to download
        image tiers that do exist.
        """
        with make_tmp_dir() as tmp_dir:
            dset_1 = mouse_utils.SectionDataSet(
                100055044,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            # try downloading good files
            tiff_name = tmp_dir / 'tiss_13.tiff'
            self.assertFalse(tiff_name.exists())
            res = dset_1.download_image_from_tissue_index(13, 4, tiff_name)
            self.assertIs(res, True)
            self.assertTrue(tiff_name.exists())
            # make sure image is valid
            f = Image.open(tiff_name)
            f.load()
            f.close()

            dset_2 = mouse_utils.SectionDataSet(
                100055049,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            # try downloading good files
            tiff_name = tmp_dir / 'tiss_101082398.tiff'
            self.assertFalse(tiff_name.exists())
            res = dset_2.download_image_from_sub_image(101082398, 4, tiff_name)
            self.assertIs(res, True)
            self.assertTrue(tiff_name.exists())
            # make sure image is valid
            f = Image.open(tiff_name)
            f.load()
            f.close()

    def test_clobber_tissue_index(self):
        """
        Test behavior of clobber kwarg in methods to download images
        """
        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            tiff_name = tmp_dir / 'clobber.tiff'
            res = dataset.download_image_from_tissue_index(58, 5, tiff_name)
            self.assertIs(res, True)

            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(10000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
            md5_0 = md5_obj.hexdigest()

            # try downloading to the same file without clobber
            with self.assertWarns(UserWarning) as bad_download:
                res = dataset.download_image_from_tissue_index(114, 4, tiff_name)
            self.assertIs(res, False)
            self.assertIn('re-run with clobber=True',
                        bad_download.warning.args[0])
            self.assertIn('%s already exists' % tiff_name,
                        bad_download.warning.args[0])

            # check that no new file was downloaded
            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(10000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
            md5_1 = md5_obj.hexdigest()
            self.assertEqual(md5_0, md5_1)

            # rerun with clobber
            res = dataset.download_image_from_tissue_index(
                114, 4, tiff_name, clobber=True
            )
            self.assertIs(res, True)

            # check that a new file was downloaded
            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(10000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
            md5_2 = md5_obj.hexdigest()
            self.assertNotEqual(md5_0, md5_2)

    def test_clobber_sub_image(self):
        """
        Test behavior of clobber kwarg in methods to download images
        """
        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            tiff_name = tmp_dir / 'clobber2.tiff'
            res = dataset.download_image_from_sub_image(102000002,
                                                        5, tiff_name)
            self.assertIs(res, True)

            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(1000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
            md5_0 = md5_obj.hexdigest()

            # try downloading to the same file without clobber
            with self.assertWarns(UserWarning) as bad_download:
                res = dataset.download_image_from_sub_image(102000008,
                                                            4, tiff_name)
            self.assertIs(res, False)
            self.assertIn('re-run with clobber=True',
                        bad_download.warning.args[0])
            self.assertIn('%s already exists' % tiff_name,
                        bad_download.warning.args[0])

            # check that no new file was downloaded
            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(1000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
                md5_1 = md5_obj.hexdigest()
            self.assertEqual(md5_0, md5_1)

            # rerun with clobber
            res = dataset.download_image_from_sub_image(102000008, 4, tiff_name,
                                                        clobber=True)
            self.assertIs(res, True)

            # check that a new file was downloaded
            md5_obj = hashlib.md5()
            with open(tiff_name, 'rb') as in_file:
                while True:
                    data = in_file.read(1000)
                    if len(data) == 0:
                        break
                    md5_obj.update(data)
            md5_2 = md5_obj.hexdigest()
            self.assertNotEqual(md5_0, md5_2)

    def test_download_into_not_a_file(self):
        """
        Test what happens when you try to download into a path
        that exists but is not a file
        """

        with make_tmp_dir() as tmp_dir:
            tiff_name = Path(tempfile.mkdtemp(dir=tmp_dir))
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            with self.assertWarns(UserWarning) as bad_download:
                res = dataset.download_image_from_sub_image(102000002,
                                                            5, tiff_name)
            self.assertIs(res, False)
            self.assertIn('%s already exists but is not a file' % tiff_name,
                        bad_download.warning.args[0])
            self.assertTrue(tiff_name.is_dir())

            with self.assertWarns(UserWarning) as bad_download:
                res = dataset.download_image_from_sub_image(102000002,
                                                            5, tiff_name,
                                                            clobber=True)
            self.assertIs(res, False)
            self.assertIn('%s already exists but is not a file' % tiff_name,
                        bad_download.warning.args[0])
            self.assertTrue(tiff_name.is_dir())

            shutil.rmtree(tiff_name)

    def test_subimg_id_and_tissue_dex(self):
        """
        Test the contents of SectionDataSet.sub_image_ids
        and SectionDataSet.tissue_indices
        """
        with make_tmp_dir() as tmp_dir:
            dataset = mouse_utils.SectionDataSet(
                self.example_section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            control = [102000002, 102000006,
                    102000008, 102000010, 102000012, 102000014,
                    102000016, 102000018, 102000020, 102000022,
                    102000024, 102000026, 102000028, 102000032,
                    102000034, 102000036, 102000038]

            self.assertEqual(dataset.sub_image_ids, control)

            control = [10, 26, 34, 42, 50, 58, 66, 74, 82, 90, 98,
                    106, 114, 130, 138, 146, 154]

            self.assertEqual(dataset.tissue_indices, control)

    def test_many_sub_images(self):
        """
        Test that metadata is properly loaded when one TIFF contains
        many sub-images
        """
        with make_tmp_dir() as tmp_dir:
            section_id = 100055044
            dataset = mouse_utils.SectionDataSet(
                section_id,
                download_directory=tmp_dir,
                s3_client=self.s3_client
            )

            control_file = (
                Path(__file__).resolve().parent
                / 'test_data'
                / 'section_data_set_100055044_metadata.json'
            )
            with open(control_file, 'rb') as in_file:
                control_metadata = json.load(in_file)

            for control_img in control_metadata['section_images']:
                tissue_index = control_img['section_number']
                subimg_id = control_img['id']
                test = dataset.image_metadata_from_tissue_index(tissue_index)
                print(test)
                self.assertEqual(control_img, test)
                test = dataset.image_metadata_from_sub_image(subimg_id)
                self.assertEqual(control_img, test)


if __name__ == "__main__":
    unittest.main()
