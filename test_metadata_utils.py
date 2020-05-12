import os
import hashlib
import aws_utils
import metadata_utils as mu
import unittest
import time
import tempfile
import shutil
import json
import PIL.Image

import warnings

class MetadataTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # regarding warnings filter, see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message='unclosed <ssl.SSLSocket')
        cls.tmp_dir = 'test_tmp'
        cls.session = aws_utils.get_boto3_session()

    def get_tmp_dir(self):
        """
        Get a unique tmp_dir for a test method
        """
        return tempfile.mkdtemp(dir=self.tmp_dir)

    def clean_tmp_dir(self, tmp_dir):
        """
        Clean up the specified tmp_dir
        """
        f_list = os.listdir(tmp_dir)
        for f in f_list:
            os.unlink(os.path.join(tmp_dir, f))
        shutil.rmtree(tmp_dir)

    def test_get_tmp_dir(self):
        """
        Test that _get_tmp_dir operates as expected
        """
        this_dir = os.path.dirname(os.path.abspath(__file__))
        tmp_dir = mu._get_tmp_dir()
        self.assertEqual(tmp_dir, os.path.join(this_dir, 'tmp'))

    def test_atlas_metadata(self):
        """
        Test that we can download the global section_data_sets.json
        file. Compare its md5 checksum to a hard-coded value taken
        from a verified copy that was downloaded from S3 by hand.
        """
        tmp_dir = self.get_tmp_dir()
        for session in (self.session, None):
            metadata = mu.get_atlas_metadata(session=session,
                                             tmp_dir=tmp_dir)

            self.assertIsInstance(metadata, list)
            self.assertEqual(len(metadata), 26078)
            for obj in metadata:
                self.assertIsInstance(obj, dict)

            fname = os.path.join(tmp_dir, 'section_data_sets.json')

            if not os.path.exists(os.path.join(fname)):
                raise RuntimeError("Failed to download section_data_sets.json")

            md5_obj = hashlib.md5()
            with open(os.path.join(fname), 'rb') as in_file:
                for line in in_file:
                    md5_obj.update(line)
            checksum = md5_obj.hexdigest()
            self.assertEqual(checksum,
                             '2c974e2be3a30a4d923f47dd4a7fde72')

            os.unlink(fname)
        self.clean_tmp_dir(tmp_dir)

    def test_section_metadata(self):
        """
        Test downloading a specific section's metadata. Verify the file
        against a hard-coded md5 checksum
        """
        tmp_dir = self.get_tmp_dir()
        section_id = 99
        for session in (self.session, None):
            metadata = mu.get_section_metadata(section_id=section_id,
                                               session=session,
                                               tmp_dir=tmp_dir)

            self.assertIsInstance(metadata, dict)

            fname = os.path.join(tmp_dir,
                                 'section_data_set_%d_metadata.json' % section_id)

            if not os.path.exists(os.path.join(fname)):
                raise RuntimeError("Failed to download %s" % fname)

            md5_obj = hashlib.md5()
            with open(os.path.join(fname), 'rb') as in_file:
                for line in in_file:
                    md5_obj.update(line)
            checksum = md5_obj.hexdigest()
            self.assertEqual(checksum,
                             'e8eff384bb39cc981f93bad62e6fad02')

            os.unlink(fname)
        self.clean_tmp_dir(tmp_dir)

    def test_download_caching(self):
        """
        Test that the method to download metadata does not download it twice
        unnecessarily
        """
        tmp_dir = self.get_tmp_dir()
        section_id = 99
        fname = os.path.join(tmp_dir,
                             'section_data_set_%d_metadata.json' % section_id)
        aws_name = 'section_data_set_%d/section_data_set.json' % section_id
        self.assertFalse(os.path.exists(fname))
        mu._get_aws_file(aws_name, fname, self.session)
        self.assertTrue(os.path.exists(fname))
        fstats = os.stat(fname)
        t0 = fstats.st_mtime_ns  # get the time of last modificatio in nanosec
        time.sleep(2)  # wait so that, if redownloaded, st_mtime_ns would differ
        mu._get_aws_file(aws_name, fname, self.session)
        fstats = os.stat(fname)
        t1 = fstats.st_mtime_ns
        self.assertEqual(t1, t0)
        self.clean_tmp_dir(tmp_dir)


class SectionDataSetTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # regarding warnings filter, see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message='unclosed <ssl.SSLSocket')
        cls.tmp_dir = tempfile.mkdtemp(dir='test_tmp')
        cls.session = aws_utils.get_boto3_session()
        cls.example_section_id = 275693
        mu.get_section_metadata(cls.example_section_id,
                                session=cls.session,
                                tmp_dir=cls.tmp_dir)

    @classmethod
    def tearDownClass(cls):
        f_list = os.listdir(cls.tmp_dir)
        for fname in f_list:
            os.unlink(os.path.join(cls.tmp_dir, fname))
        shutil.rmtree(cls.tmp_dir)

    def test_metadata_from_tissue_index(self):
        """
        Try loading the metadata by tissue_index.
        Compare to a json dict of the expected result that was
        copied to test_data/ by hand.
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        metadata = dataset.image_metadata_from_tissue_index(154)
        control_file = os.path.join('test_data',
                                    'example_metadata_tissue_154.json')
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
        copied to test_data/ by hand.
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        metadata = dataset.image_metadata_from_sub_image(102000022)
        control_file = os.path.join('test_data',
                                    'example_metadata_id_102000022.json')
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
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        # verify what happens when you ask for a resolution
        # that does not exist
        tiff_name = os.path.join(self.tmp_dir, 'junk.tiff')
        with self.assertWarns(UserWarning) as bad_image:
            res = dataset.download_image_from_tissue_index(66,
                                                           0,
                                                           tiff_name)
        self.assertIs(res, False)
        self.assertFalse(os.path.exists(tiff_name))
        self.assertIn("0 is not a valid downsampling tier",
                      bad_image.warning.args[0])
        metadata = dataset.image_metadata_from_tissue_index(66)
        self.assertNotIn('downsample_0', metadata)

        with self.assertWarns(UserWarning) as bad_image:
            res = dataset.download_image_from_sub_image(102000016,
                                                        0,
                                                        tiff_name)
        self.assertIs(res, False)
        self.assertFalse(os.path.exists(tiff_name))
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
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        # verify what happens when you ask for a resolution
        # that does not exist
        tiff_name = os.path.join(self.tmp_dir, 'junk2.tiff')
        with self.assertWarns(UserWarning) as bad_image:
            res = dataset.download_image_from_tissue_index(999,
                                                           4,
                                                           tiff_name)
        self.assertIs(res, False)
        self.assertFalse(os.path.exists(tiff_name))
        self.assertIn("tissue_index 999 does not exist",
                      bad_image.warning.args[0])

        with self.assertWarns(UserWarning) as bad_image:
            res = dataset.download_image_from_sub_image(999,
                                                        4,
                                                        tiff_name)
        self.assertIs(res, False)
        self.assertFalse(os.path.exists(tiff_name))
        self.assertIn("sub_image 999 does not exist",
                      bad_image.warning.args[0])

    def test_good_tier_image_download(self):
        """
        Test behavior of SectionDataSet when you ask it to download
        image tiers that do exist.
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        # try downloading good files
        tiff_name = os.path.join(self.tmp_dir, 'tiss_66.tiff')
        self.assertFalse(os.path.exists(tiff_name))
        res = dataset.download_image_from_tissue_index(66, 6, tiff_name)
        self.assertIs(res, True)
        self.assertTrue(os.path.exists(tiff_name))
        # make sure image is valid
        f = PIL.Image.open(tiff_name)
        f.load()
        f.close()

        # try downloading good files
        tiff_name = os.path.join(self.tmp_dir, 'tiss_102000002.tiff')
        self.assertFalse(os.path.exists(tiff_name))
        res = dataset.download_image_from_sub_image(102000002, 6, tiff_name)
        self.assertIs(res, True)
        self.assertTrue(os.path.exists(tiff_name))
        # make sure image is valid
        f = PIL.Image.open(tiff_name)
        f.load()
        f.close()

    def test_clobber_tissue_index(self):
        """
        Test behavior of clobber kwarg in methods to download images
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        tiff_name = os.path.join(self.tmp_dir, 'clobber.tiff')
        res = dataset.download_image_from_tissue_index(58, 5, tiff_name)
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
                data = in_file.read(1000)
                if len(data) == 0:
                    break
                md5_obj.update(data)
        md5_1 = md5_obj.hexdigest()
        self.assertEqual(md5_0, md5_1)

        # rerun with clobber
        res = dataset.download_image_from_tissue_index(114, 4, tiff_name,
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

    def test_clobber_sub_image(self):
        """
        Test behavior of clobber kwarg in methods to download images
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        tiff_name = os.path.join(self.tmp_dir, 'clobber2.tiff')
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


if __name__ == "__main__":
    unittest.main()
