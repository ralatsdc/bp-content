# -*- coding: utf-8 -*-

# Standard library imports
import unittest

# Third-party imports

# Local imports
from TumblrUtility import TumblrUtility

class TumblrUtilityTestCase(unittest.TestCase):

    def setUp(self):
        self.tumblr_utility = TumblrUtility()

    def tearDown(self):
        pass

    def test_get_names_from_term(self):
        names = self.tumblr_utility.get_names_from_term("food")
        self.assertTrue(len(names) > 0)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TumblrUtilityTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
