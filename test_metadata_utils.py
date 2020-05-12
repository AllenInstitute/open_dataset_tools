import os
import hashlib
import aws_utils
import metadata_utils as mu
import unittest

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
        metadata = mu.get_atlas_metadata(session=self.session,
                                         tmp_dir=self.tmp_dir)

        self.assertIsInstance(metadata, list)
        self.assertEqual(len(metadata), 26078)
        for obj in metadata:
            self.assertIsInstance(obj, dict)

        fname = os.path.join(self.tmp_dir, 'section_data_sets.json')

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

    def test_section_metadata(self):
        """
        Test downloading a specific section's metadata. Verify the file
        against a hard-coded md5 checksum
        """
        section_id = 99
        metadata = mu.get_section_metadata(section_id=section_id,
                                           session=self.session,
                                           tmp_dir=self.tmp_dir)

        self.assertIsInstance(metadata, dict)

        fname = os.path.join(self.tmp_dir,
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

if __name__ == "__main__":
    unittest.main()
