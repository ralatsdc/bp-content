# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime
import logging
import math
import os
import pickle
from time import sleep

# Third-party imports
from instagram.client import InstagramAPI

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class InstagramAuthor:
    """Represents an author on Instagram by their creative
    output. Authors are selected by tag.

    """
    def __init__(self, blu_pen, source_words_str, content_dir, requested_dt=datetime.utcnow(),
                 client_id="690d3be7af514fd3942266b6cfc04388", client_secret="a73c94a839b441bea37ea3b94eb1907a",
                 number_of_api_attempts=1, seconds_between_api_attempts=1):

        """Constructs a InstagramAuthor instance.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utl = BluePeninsulaUtility()

        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_types,
         self.source_words) = self.blu_pen_utl.process_source_words(source_words_str)
        if len(self.source_words) > 1:
            err_msg = "{0} only one source word accepted".format(self.source_path)
            self.logger.error(err_msg)
            raise Exception(msg)

        self.content_dir = content_dir
        self.requested_dt = requested_dt
        self.client_id = client_id
        self.client_secret = client_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        self.api = InstagramAPI(client_id=client_id, client_secret=client_secret)
        self.logger = logging.getLogger(__name__)
        
    def set_media_as_recent(self, count=None, max_id=None):
        """Gets a list of media recently tagged with the specified tag
        and parses the result.

        """
        # TODO: Get, parse, assign, etc.
        envelope = self.get_media_by_source(self.source_types[0], self.source_words[0])
        self.content_set = True

    def get_media_by_source(self, source_type, source_word, count=None, max_id=None):
        """Makes multiple attempts to get source content, sleeping
        before attempts.

        """
        envelope = None

        # Make multiple attempts
        iAttempts = 0
        while envelope is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep longer before each attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0} sleeping for {1} seconds".format(self.source_log, seconds_between_api_attempts))
            sleep(seconds_between_api_attempts)

            # Handle errors
            try:

                # Make an API request based on source type
                if source_type == "@":

                    # Get the most recent media published by a user: currently not supported
                    self.logger.warning("{0} couldn't get content for {1}{2}: {3}".format(
                        self.source_log, source_type, source_word, exc))

                else: # source_type == "#":

                    # Get a list of recently tagged media
                    if count is not None and max_id is not None:
                        envelope = self.api.tag_recent_media(tag_name=source_word, count=count, max_id=max_id)

                    elif count is not None:
                        envelope = self.api.tag_recent_media(tag_name=source_word, count=count)

                    elif max_id is not None:
                        envelope = self.api.tag_recent_media(tag_name=source_word, max_id=max_id)

                    else:
                        envelope = self.api.tag_recent_media(tag_name=source_word)

            except Exception as exc:
                self.logger.warning("{0} couldn't get content for {1}{2}: {3}".format(
                        self.source_log, source_type, source_word, exc))

        return envelope

    def dump(self, pickle_file_name=None):
        """Dump InstagramAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_log'] = self.source_log
        p['source_path'] = self.source_path
        p['source_header'] = self.source_header
        p['source_label'] = self.source_label
        p['source_types'] = self.source_types
        p['source_words'] = self.source_words

        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['client_id'] = self.client_id
        p['client_secret'] = self.client_secret
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped content to {1}".format(
            self.source_log, pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load InstagramAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_log = p['source_log']
        self.source_path = p['source_path']
        self.source_header = p['source_header']
        self.source_label = p['source_label']
        self.source_types = p['source_types']
        self.source_words = p['source_words']

        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.client_id = p['client_id']
        self.client_secret = p['client_secret']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.logger.info("{0} loaded {1} photosets from {2}".format(
                self.source_log, len(self.photosets), pickle_file_name))

        pickle_file.close()
