# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime, date, timedelta
import logging
import os
import pickle
from urlparse import urlparse

# Third-party imports
import sys
sys.path.append('../../lib/feedparser/feedparser')
import feedparser

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class FeedAuthor:
    """Represents authors of Feeds by their creative output. Authors
    are selected by URL.

    """
    def __init__(self, blu_pen, source_url, content_dir, requested_dt=datetime.utcnow()):
        """Constructs a FeedAuthor instance.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utl = BluePeninsulaUtility()

        self.source_url = source_url
        self.content_dir = content_dir
        self.requested_dt = requested_dt

        self.source_netloc = urlparse(self.source_url).netloc
        self.pickle_file_name = os.path.join(self.content_dir, self.source_netloc + ".pkl")

        self.content = None
        self.feed = {}
        self.entries = []
        self.content_set = False

        self.logger = logging.getLogger("blu-pen.FeedAuthor")

    def set_content_as_recent(self):
        """Gets the feed content from the source URL, then assigns
        values to selected keys.

        """
        # Get feed content
        try:
            self.content = feedparser.parse(self.source_url)
        except Exception as exc:
            self.logger.info("{0} could not parse feed content from {1}".format(self.source_netloc, self.source_url))
            raise Exception("Problem with feed")

        # Set feed content, if it exsits
        if 'feed' in self.content:

            # Assign feed keys to set
            feed_keys = ["title", "author", "publisher",
                         "published_parsed", "update_parsed",
                         "license"]

            # Assign value for key, if in feed, or None
            for key in feed_keys:
                if key in self.content['feed']:
                    self.feed[key] = self.content['feed'][key]
                else:
                    self.feed[key] = None

        # Set entries content, if it exsits
        if 'entries' in self.content:

            # Assign entry keys to set
            entry_keys = ["title", "author", "publisher",
                          "published_parsed", "created_parsed", "expired_parsed", "updated_parsed",
                          "license"]

            # Assign value for key, if in entry, or None
            for entry in self.content['entries']:
                self.entries.append({})
                for key in entry_keys:
                    if key in entry:
                        self.entries[-1][key] = entry[key]
                    else:
                        self.entries[-1][key] = None

        self.content_set = True

    def download_images(self):
        """Downloads all images by this author from the feed.

        """
        for entry in self.content['entries']:
            for link in entry['links']:
                if link['type'].find('image/') != -1:
                    image_url = link['href']
                    head, tail = os.path.split(image_url)
                    image_file_name = os.path.join(self.content_dir, tail)
                    self.blu_pen_utl.download_file(image_url, image_file_name)
                    self.logger.info("{0} downloaded image to file {1}".format(
                        self.source_netloc, image_file_name))

    def dump(self, pickle_file_name=None):
        """Dumps FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_url'] = self.source_url
        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt

        p['source_netloc'] = self.source_netloc
        p['pickle_file_name'] = self.pickle_file_name

        p['content'] = self.content
        p['feed'] = self.feed
        p['entries'] = self.entries
        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped content to {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_url = p['source_url']
        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']

        self.source_netloc = p['source_netloc']
        self.pickle_file_name = p['pickle_file_name']

        self.content = p['content']
        self.feed = p['feed']
        self.entries = p['entries']
        self.content_set = p['content_set']

        self.logger.info("{0} loaded content from {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()
