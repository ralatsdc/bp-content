# -*- coding: utf-8 -*-

# Standard library imports
from StringIO import StringIO
import codecs
from datetime import datetime, date, timedelta
import logging
import os
import pickle
from pprint import pprint
from urlparse import urlparse

# Third-party imports
import sys
sys.path.append('../../lib/feedparser/feedparser')
import feedparser
from lxml import etree

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class FeedAuthor:
    """Represents authors of Feeds by their creative output. Authors
    are selected by URL.

    """
    def __init__(self, blu_pen, source_url, content_dir, requested_dt=datetime.utcnow()):
        """Constructs a FeedAuthor instance given source words.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utl = BluePeninsulaUtility()
        self.source_url = source_url
        self.source_netloc = urlparse(self.source_url).netloc
        
        self.content_dir = content_dir
        self.pickle_file_name = os.path.join(self.content_dir, self.source_netloc + ".pkl")

        self.requested_dt = requested_dt

        self.content = None
        self.content_set = False

        self.feed = {}
        self.entries = []

        self.logger = logging.getLogger("blu-pen.FeedAuthor")

    def set_content_as_recent(self):
        """Gets feed content from source URL.

        """
        # Get feed content
        try:
            self.content = feedparser.parse(self.source_url)
        except Exception as exc:
            self.logger.info("{0} could not parse feed content from {1}".format(self.source_netloc, self.source_url))
            raise Exception("Problem with feed")
        self.content_set = True

        if 'feed' in self.content:

            feed_keys = ["title", "author", "publisher",
                         "published_parsed", "update_parsed",
                         "license"]

            def set_feed_value_by_key(key):
                if key in self.content['feed']:
                    self.feed[key] = self.content['feed'][key]
                else:
                    self.feed[key] = None

            for key in feed_keys:
                set_feed_value_by_key(key)

        if 'entries' in self.content:

            entry_keys = ["title", "author", "publisher",
                          "published_parsed", "created_parsed", "expired_parsed", "updated_parsed",
                          "license"]

            def set_entry_value_by_key(key, entry):
                if key in entry:
                    self.entries[-1][key] = entry[key]
                else:
                    self.entries[-1][key] = None

            for entry in self.content['entries']:
                self.entries.append({})
                for key in entry_keys:
                    set_entry_value_by_key(key, entry)

    def download_images(self):
        """Download all images by this author from the feed.

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
        p['source_netloc'] = self.source_netloc

        p['content_dir'] = self.content_dir
        p['pickle_file_name'] = self.pickle_file_name

        p['requested_dt'] = self.requested_dt

        p['content'] = self.content
        p['content_set'] = self.content_set

        p['feed'] = self.feed
        p['entries'] = self.entries

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
        self.source_netloc = p['source_netloc']

        self.content_dir = p['content_dir']
        self.pickle_file_name = p['pickle_file_name']

        self.requested_dt = p['requested_dt']

        self.content = p['content']
        self.content_set = p['content_set']

        self.feed = p['feed']
        self.entries = ['entries']

        self.logger.info("{0} loaded content from {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()
