# -*- coding: utf-8 -*-

# Standard library imports
import unittest

# Third-party imports

# Local imports
from TwitterUtility import TwitterUtility

class TwitterUtilityTestCase(unittest.TestCase):

    def setUp(self):
        self.twitter_utility = TwitterUtility()

    def tearDown(self):
        pass

    def test_get_names_from_term(self):
        names = self.twitter_utility.get_names_from_term("happy")
        self.assertTrue(len(names) > 0)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TwitterUtilityTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
