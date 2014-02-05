# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime
import logging
import math
import os
import pickle
from time import sleep
from urlparse import urlparse

# Third-party imports
import feedparser
from lxml.html import soupparser

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class FeedAuthor:
    """Represents authors of Feeds by their creative output. Authors
    are selected by URL.

    """
    def __init__(self, blu_pen, source_url, content_dir, requested_dt=datetime.utcnow(),
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a FeedAuthor instance.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utl = BluePeninsulaUtility()

        self.source_log = urlparse(source_url).netloc
        self.source_path = urlparse(source_url).netloc
        self.source_url = source_url

        self.content_dir = content_dir
        self.requested_dt = requested_dt
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        self.content = None
        self.feed = {}
        self.entries = []
        self.content_set = False

        self.logger = logging.getLogger("blu-pen.FeedAuthor")

    def set_content_as_recent(self):
        """Gets the feed content from the source URL, then assigns
        values to selected keys.

        """
        # Get recent feed content
        self.content = self.get_content_by_url(self.source_url)
        if self.content is None:
            return

        # Set feed content, if it exsits
        if 'feed' in self.content:

            # Assign feed keys to set
            feed_keys = ["title", "author", "publisher",
                         "published_parsed", "update_parsed", "license"]

            # Assign value for key, if in feed, or None
            for key in feed_keys:
                if key in self.content['feed']:
                    self.feed[key] = self.content['feed'][key]
                else:
                    self.feed[key] = None

        # Set entries content, if it exsits
        if 'entries' in self.content:

            # Assign entry keys to set
            entry_keys = ["title", "author", "publisher", "content",
                          "published_parsed", "created_parsed", "expired_parsed", "updated_parsed", "license"]

            # Assign value for key, if in entry, or None
            for entry in self.content['entries']:
                self.entries.append({})
                for key in entry_keys:
                    if key in entry:
                        self.entries[-1][key] = entry[key]
                    else:
                        self.entries[-1][key] = None

        self.content_set = True

    def get_content_by_url(self, source_url):
        """Makes multiple attempts to get content by URL, sleeping
        before attempts.

        """
        content = None

        # Make multiple attempts
        exc = None
        iAttempts = 0
        while content is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep longer before each attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0} sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            sleep(seconds_between_api_attempts)

            # Attempt to get content by URL
            try:
                content = feedparser.parse(source_url)
                self.logger.info("{0} collected content for {1}".format(
                    self.source_log, source_url))
            except Exception as exc:
                content = None
                self.logger.warning("{0} couldn't get content for {1}: {2}".format(
                    self.source_log, source_url, exc))

        return content

    def set_image_urls_as_recent(self):
        """Find and set image URLs for each entry of type 'text/html'.

        """
        # Consider each entry
        for entry in self.entries:
            content = entry['content'][0]
            content['image_urls'] = []

            # Convert 'text/html' entries only
            if content['type'] == 'text/html':

                # Parse the HTML content
                root = soupparser.fromstring(content['value'])

                # Consider each element
                for element in root.iter():

                    # Process image elements only
                    if element.tag == 'img':

                        # Append the image URL
                        content['image_urls'].append(element.get('src'))

    def download_images(self):
        """Download all content images for each entry.

        """
        # Consider each entry
        for entry in self.entries:
            content = entry['content'][0]
            content['image_file_names'] = []

            # Consider each image URLs
            for image_url in content['image_urls']:

                # Append the image file name
                head, tail = os.path.split(image_url)
                image_file_name = os.path.join(self.content_dir, tail)
                content['image_file_names'].append(image_file_name)

                # Download image to file
                if not os.path.exists(image_file_name):
                    self.blu_pen_utl.download_file(image_url, image_file_name)
                    self.logger.info("{0} image downloaded to file {1}".format(
                        self.source_path, image_file_name))
                else:
                    self.logger.info("{0} image already downloaded to file {1}".format(
                        self.source_path, image_file_name))

    def dump(self, pickle_file_name=None):
        """Dumps FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_log'] = self.source_log
        p['source_path'] = self.source_path
        p['source_url'] = self.source_url

        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['content'] = self.content
        p['feed'] = self.feed
        p['entries'] = self.entries
        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped content to {1}".format(
            self.source_log, pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_log = p['source_log']
        self.source_path = p['source_path']
        self.source_url = p['source_url']

        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.content = p['content']
        self.feed = p['feed']
        self.entries = p['entries']
        self.content_set = p['content_set']

        self.logger.info("{0} loaded content from {1}".format(
            self.source_log, pickle_file_name))

        pickle_file.close()
