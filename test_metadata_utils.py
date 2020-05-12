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

class AWSUtilsTestCase(unittest.TestCase):

    def test_get_keys_no_file(self):
        """
        Test what happens when you do not specify a key
        name for get_aws_keys
        """
        with self.assertRaises(RuntimeError) as bad_run:
            aws_utils.get_aws_keys(None)
        self.assertEqual('Must specify filename in get_aws_keys',
                         bad_run.exception.args[0])

    def test_get_keys_not_a_file(self):
        """
        Test what happens when you specify something that is not
        a file in get_aws_keys
        """
        dir_name = tempfile.mkdtemp(dir='test_tmp')
        with self.assertRaises(RuntimeError) as bad_run:
            aws_utils.get_aws_keys(dir_name)
        self.assertEqual('\n%s\nis not a file' % dir_name,
                         bad_run.exception.args[0])

        shutil.rmtree(dir_name)

    def test_get_keys_bad_file(self):
        """
        Test what happens when the expected values are not in the
        accessKeys.csv file
        """
        fname = tempfile.mkstemp(dir='test_tmp', suffix='.csv')[1]
        with open(fname, 'w') as out_file:
            out_file.write('a,b,c,d\n')
            out_file.write('1,2,3,4\n')

        with self.assertRaises(RuntimeError) as bad_run:
            aws_utils.get_aws_keys(fname)
        self.assertIn("Could not find 'Access key ID'",
                      bad_run.exception.args[0])
        self.assertIn("Could not find 'Secret access key'",
                      bad_run.exception.args[0])
        self.assertIn(fname, bad_run.exception.args[0])

        os.unlink(fname)

    def test_dummy_keys(self):
        """
        Test reading keys from a properly formatted file
        """
        fname = tempfile.mkstemp(dir='test_tmp', suffix='.csv')[1]
        key_id = 'meringue'
        secret_key = 'blueberry'
        with open(fname, 'w') as out_file:
            out_file.write('a,b,Secret access key,c,d,Access key ID,e\n')
            out_file.write('apple,banana,%s,2,4,%s,9\n' %
                           (secret_key, key_id))

        read_id, read_key = aws_utils.get_aws_keys(fname)
        self.assertEqual(read_id, key_id)
        self.assertEqual(read_key, secret_key)
        os.unlink(fname)


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

    def test_download_into_not_a_file(self):
        """
        Test what happens when you try to download into a path
        that exists but is not a file
        """
        tiff_name = tempfile.mkdtemp(dir=self.tmp_dir)
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        with self.assertWarns(UserWarning) as bad_download:
            res = dataset.download_image_from_sub_image(102000002,
                                                        5, tiff_name)
        self.assertIs(res, False)
        self.assertIn('%s already exists but is not a file' % tiff_name,
                      bad_download.warning.args[0])
        self.assertTrue(os.path.isdir(tiff_name))

        with self.assertWarns(UserWarning) as bad_download:
            res = dataset.download_image_from_sub_image(102000002,
                                                        5, tiff_name,
                                                        clobber=True)
        self.assertIs(res, False)
        self.assertIn('%s already exists but is not a file' % tiff_name,
                      bad_download.warning.args[0])
        self.assertTrue(os.path.isdir(tiff_name))


        shutil.rmtree(tiff_name)

    def test_subimg_id_and_tissue_dex(self):
        """
        Test the contents of SectionDataSet.sub_image_ids
        and SectionDataSet.tissue_indices
        """
        dataset = mu.SectionDataSet(self.example_section_id,
                                    session=self.session,
                                    tmp_dir=self.tmp_dir)

        control = [102000002, 102000006,
                   102000008, 102000010, 102000012, 102000014,
                   102000016, 102000018, 102000020, 102000022,
                   102000024, 102000026, 102000028, 102000032,
                   102000034, 102000036, 102000038]

        self.assertEqual(dataset.sub_image_ids, control)

        control = [10, 26, 34, 42, 50, 58, 66, 74, 82, 90, 98,
                   106, 114, 130, 138, 146, 154]

        self.assertEqual(dataset.tissue_indices, control)


if __name__ == "__main__":
    unittest.main()
