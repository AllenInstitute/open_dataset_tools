import os
import hashlib
import aws_utils
import metadata_utils as mu
import unittest
import time
import tempfile
import shutil

import warnings

class MetadataTestCase(unittest.TestCase):

    @classmethod
    def cleanUpTmp(cls):
        # clean out tmp_test
        tmp_file_list = os.listdir(cls.tmp_dir)
        for fname in tmp_file_list:
            if fname.startswith('.'):
                continue
            full_name = os.path.join(cls.tmp_dir, fname)
            os.unlink(full_name)

    @classmethod
    def setUpClass(cls):
        # see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message='unclosed <ssl.SSLSocket')
        cls.tmp_dir = 'test_tmp'
        cls.cleanUpTmp()
        cls.session = aws_utils.get_boto3_session()
        mu.get_section_metadata(275693, session=cls.session,
                                tmp_dir=cls.tmp_dir)

    @classmethod
    def tearDownClass(cls):
        cls.cleanUpTmp()

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


if __name__ == "__main__":
    unittest.main()
