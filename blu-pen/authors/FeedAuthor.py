# -*- coding: utf-8 -*-

# Standard library imports
from StringIO import StringIO
import codecs
from datetime import datetime, date, timedelta
import logging
import os
import pickle
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

        self.authors = []
        self.start_dt = None
        self.stop_dt = None
        self.tags = set()

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

        # Determine authors
        self.authors = []
        for author in self.content['feed']['authors']:
            self.authors.append(author['name'])
            
        # Determine start and stop datetime, and unique tags
        self.start_dt = datetime(2500, 1, 1)
        self.stop_dt = datetime(1500, 1, 1)
        for entry in self.content['entries']:
            root = etree.fromstring(entry['content'][0]['value'])
            for element in root.iter():

                # Determine start and stop time
                updated_dt = datetime.strptime(entry['updated'][0:19], '%Y-%m-%dT%H:%M:%S')
                if updated_dt < self.start_dt:
                    self.start_dt = updated_dt
                if updated_dt > self.stop_dt:
                    self.stop_dt = updated_dt

                # Determine unique tags
                if not element.tag in self.tags and not element.tag == etree.Comment:
                    self.tags.add(element.tag)
        
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

        p['authors'] = self.authors
        p['start_dt'] = self.start_dt
        p['stop_dt'] = self.stop_dt
        p['tags'] = self.tags

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

        self.authors = p['authors']
        self.start_dt = p['start_dt']
        self.stop_dt = p['stop_dt']
        self.tags = p['tags']

        self.logger.info("{0} loaded content from {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()
