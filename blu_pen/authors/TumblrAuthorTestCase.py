# -*- coding: utf-8 -*-

# Standard library imports
import os
import unittest

# Third-party imports

# Local imports
from TumblrAuthor import TumblrAuthor

class TumblrAuthorTestCase(unittest.TestCase):

    def setUp(self):
        """Initialize two TumblrAuthors with the same known subdomain.

        """
        self.subdomain = "ralattnc"
        self.content_dir = "../content"
        self.tumblr_author_a = TumblrAuthor(self.subdomain, self.content_dir)
        self.tumblr_author_b = TumblrAuthor(self.subdomain, self.content_dir)

    def tearDown(self):
        pass

    def test_set_as_recent(self):
        """Get posts for the first TumblrAuthor from Tumblr, and load
        the posts for the second TumblrAuthor from its pickle. Compare
        a few posts to hard coded values, and posts and attributes to
        those in the pickle.

        """
        self.tumblr_author_a.set_posts_as_recent()

        self.assertEquals(
            self.tumblr_author_a.posts[0]['regular-body'],
            "<p>is doing better than the media suggests, though there are problems, some long standing&#8230;</p>")
        self.assertEquals(
            self.tumblr_author_a.posts[2]['regular-body'],
            "<p>lost my user manual.</p>")

        pickle_file_name = os.path.join(self.content_dir, self.subdomain + ".pkl")
        self.tumblr_author_b.load(pickle_file_name)

        self.assertEqual(self.tumblr_author_a.posts,
                         self.tumblr_author_b.posts)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TumblrAuthorTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
