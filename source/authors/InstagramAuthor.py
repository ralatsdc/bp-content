# -*- coding: utf-8 -*-

# Standard library imports
import datetime
import logging
import math
import os
import pickle
import time

# Third-party imports
from instagram.client import InstagramAPI

# Local imports
from utility.AuthorsUtility import AuthorsUtility

class InstagramAuthor:
    """Represents an author on Instagram by their creative
    output.

    """
    def __init__(self, blu_pen_author, source_word_str, content_dir,
                 requested_dt=datetime.datetime.utcnow(),
                 client_id="690d3be7af514fd3942266b6cfc04388",
                 client_secret="a73c94a839b441bea37ea3b94eb1907a",
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a InstagramAuthor instance.

        """
        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.blu_pen_author = blu_pen_author
        self.authors_utility = AuthorsUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.authors_utility.process_source_words(source_word_str)
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")
        self.requested_dt = requested_dt
        self.client_id = client_id
        self.client_secret = client_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        # Initialize created attributes
        self.media_dicts = []

        # Create an API
        self.api = InstagramAPI(client_id=client_id, client_secret=client_secret)

        # Create a logger
        self.logger = logging.getLogger(__name__)
        
        # Check input arguments
        if not self.source_type == u'#':
            err_msg = u"{0} can only search by tag (#)".format(
                self.source_log)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_log)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))

    def set_media(self, count=None, max_id=None, do_purge=False):
        """Gets a list of media recently tagged with the specified tag
        and parses the result.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the FlickrAuthor pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0} getting content for {1}{2}".format(
                self.source_log, self.source_type, self.source_word))

            # Get source media
            self.media_dicts = self.get_media_by_source(self.source_type, self.source_word)
            self.content_set = True

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_media(self, source_type, source_word, count=None, max_id=None):
        """Makes multiple attempts to get source media, sleeping
        before attempts.

        """
        media_dicts = None

        # Make multiple attempts to get source media
        iAttempts = 0
        while media_dicts is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempts
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make attempt to get source media
            try:

                # Make an API request based on source type
                if source_type == u'@':

                    # Get the most recent media published by a user: currently not supported
                    self.logger.warning(u"{0} couldn't get content for {1}{2}: {3}".format(
                        self.source_log, source_type, source_word, exc))

                else: # source_type == u'#':

                    # Get a list of recently tagged media
                    if count is not None and max_id is not None:
                        (media_models, url) = self.api.tag_recent_media(tag_name=source_word, count=count, max_id=max_id)

                    elif count is not None:
                        (media_models, url) = self.api.tag_recent_media(tag_name=source_word, count=count)

                    elif max_id is not None:
                        (media_models, url) = self.api.tag_recent_media(tag_name=source_word, max_id=max_id)

                    else:
                        (media_models, url) = self.api.tag_recent_media(tag_name=source_word)
                        
                    # Append to list of media and assign relevant values
                    for media_model in media_models:
                        media_dicts.append({})
                        if media_model.images is not None:
                            media_dicts[-1]['width'] = media_model.images['standard_resolution'].width
                            media_dicts[-1]['height'] = media_model.images['standard_resolution'].height
                            media_dicts[-1]['image_url'] = media_model.images['standard_resolution'].url
                        if media_model.caption is not None:
                            media_dicts[-1]['text'] = media_model.caption.text
                            media_dicts[-1]['created_at'] = media_model.caption.created_at

                    self.logger.info(u"{0} collected {1} media object(s) for {2}{3}".format(
                        self.source_log, len(media_dicts), source_type, source_word))

            except Exception as exc:
                media_dicts = None
                self.logger.warning(u"{0} couldn't get content for {1}{2}: {3}".format(
                        self.source_log, source_type, source_word, exc))

        return media_dicts

    def download_images(self):
        """Download images for each media dictionary.

        """
        # Consider each media dictionary
        for media_dict in self.media_dicts:

            # Assign the image file name
            image_url = media_dict['image_url']
            head, tail = os.path.split(image_url)
            image_file_name = os.path.join(self.content_dir, tail)
            media_dict['image_file_name'] = image_file_name

            # Download image to file
            if not os.path.exists(image_file_name):
                self.authors_utility.download_file(image_url, image_file_name)
                self.logger.info(u"{0} image downloaded to file {1}".format(
                    self.source_path, image_file_name))
            else:
                self.logger.info(u"{0} image already downloaded to file {1}".format(
                    self.source_path, image_file_name))

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
        p['source_type'] = self.source_type
        p['source_word'] = self.source_word

        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['client_id'] = self.client_id
        p['client_secret'] = self.client_secret
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['media_dicts'] = self.media_dicts

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped content to {1}".format(
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
        self.source_type = p['source_type']
        self.source_word = p['source_word']

        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.client_id = p['client_id']
        self.client_secret = p['client_secret']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.media_dicts = p['media_dicts']

        self.logger.info(u"{0} loaded {1} photosets from {2}".format(
                self.source_log, len(self.photosets), pickle_file_name))

        pickle_file.close()
