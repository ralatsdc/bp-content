# -*- coding: utf-8 -*-

# Standard library imports
import os
import unittest

# Third-party imports

# Local imports
from FlickrAuthor import FlickrAuthor

class FlickrAuthorTestCase(unittest.TestCase):

    def setUp(self):
        """Initialize two FlickrAuthors with the same known username.

        """
        self.username = "ralattnc"
        self.content_dir = "../content"
        self.flickr_author_a = FlickrAuthor(self.username, self.content_dir)
        self.flickr_author_b = FlickrAuthor(self.username, self.content_dir)

    def tearDown(self):
        pass

    def test_set_as_recent(self):
        """Get photosets and photos for the first FlickrAuthor from
        Flickr, and load the photosets and photos for the second
        FlickrAuthor from its pickle. Compare a few photosets to hard
        coded values, and photosets to those in the pickle.

        """
        self.flickr_author_a.set_photosets_as_recent()
        self.flickr_author_a.set_photos_as_recent()
        self.flickr_author_a.download_photos()

        self.assertEqual(self.flickr_author_a.photosets[0]['title'],
                         "Geneva")
        self.assertEqual(self.flickr_author_a.photosets[0]['description'],
                         "And surround.")

        self.assertEqual(self.flickr_author_a.photosets[-1]['title'],
                         "Shed")
        self.assertEqual(self.flickr_author_a.photosets[1]['description'],
                         "For chickens.")

        self.assertEqual(self.flickr_author_a.photosets[0]['photos'][0]['datetaken'],
                         "2007-10-21 17:40:38")
        self.assertEqual(self.flickr_author_a.photosets[0]['photos'][-1]['datetaken'],
                         "2007-10-21 15:37:26")

        pickle_file_name = os.path.join(self.content_dir, self.username + ".pkl")
        self.flickr_author_b.load(pickle_file_name)

        self.assertEqual(self.flickr_author_a.photosets,
                         self.flickr_author_b.photosets)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(FlickrAuthorTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
