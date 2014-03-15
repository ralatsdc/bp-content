# -*- coding: utf-8 -*-

# Standard library imports
import os
import unittest

# Third-party imports

# Local imports
from TwitterAuthor import TwitterAuthor

class TwitterAuthorTestCase(unittest.TestCase):

    def setUp(self):
        """Initialize two TwitterAuthors with the same known screen name.

        """
        self.screen_name = "ralatepc"
        self.content_dir = "../content"
        self.twitter_author_a = TwitterAuthor(self.screen_name, self.content_dir)
        self.twitter_author_b = TwitterAuthor(self.screen_name, self.content_dir)

    def tearDown(self):
        pass

    def test_set_as_recent(self):
        """Get tweets and images for the first TwitterAuthor from
        Twitter, and load the tweets and images for the second
        TwitterAuthor from its pickle. Compare a few tweets to hard
        coded values, and tweets and attributes to those in the
        pickle.

        """
        self.twitter_author_a.set_tweets_as_recent()
        self.twitter_author_a.set_images_as_recent()

        self.assertEquals(self.twitter_author_a.tweets[0].text, "Tell me about your school.")
        self.assertEquals(self.twitter_author_a.tweets[16].text, "Wed Jan 26 21:59:37 EST 2011")

        pickle_file_name = os.path.join(self.content_dir, self.screen_name + ".pkl")
        self.twitter_author_b.load(pickle_file_name)

        self.assertEqual(self.twitter_author_a.tweets,
                         self.twitter_author_b.tweets)
        self.assertEqual(self.twitter_author_a.created_dt,
                         self.twitter_author_b.created_dt)
        self.assertEqual(self.twitter_author_a.clean_text,
                         self.twitter_author_b.clean_text)
        self.assertEqual(self.twitter_author_a.sidebar_fill_rgb,
                         self.twitter_author_b.sidebar_fill_rgb)
        self.assertEqual(self.twitter_author_a.link_rgb,
                         self.twitter_author_b.link_rgb)
        self.assertEqual(self.twitter_author_a.text_rgb,
                         self.twitter_author_b.text_rgb)
        self.assertEqual(self.twitter_author_a.background_rgb,
                         self.twitter_author_b.background_rgb)

        self.assertEqual(self.twitter_author_a.volume.tolist(),
                         self.twitter_author_b.volume.tolist())
        self.assertEqual(self.twitter_author_a.frequency,
                         self.twitter_author_b.frequency)

        self.assertEqual(self.twitter_author_a.profile_image_file_name,
                         self.twitter_author_b.profile_image_file_name)
        self.assertEqual(self.twitter_author_a.background_image_file_name,
                         self.twitter_author_b.background_image_file_name)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TwitterAuthorTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
