# -*- coding: utf-8 -*-

# Standard library imports
import unittest

# Third-party imports

# Local imports
from FlickrUtility import FlickrUtility

class FlickrUtilityTestCase(unittest.TestCase):

    def setUp(self):
        self.flickr_utility = FlickrUtility()

    def tearDown(self):
        pass

    def test_get_names_from_term(self):
        names = self.flickr_utility.get_names_from_term("happy")
        self.assertTrue(len(names) > 0)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(FlickrUtilityTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
