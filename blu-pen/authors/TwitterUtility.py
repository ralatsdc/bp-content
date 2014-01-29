# -*- coding: utf-8 -*-

# Standard library imports
import json
import logging
import re
import zipfile

# Third-party imports

# Local imports
from ServiceError import ServiceError

class TwitterUtility:
    """Provides utilities for using Twitter.

    """
    def __init__(self):
        """Constructs a TwitterUtility.

        """
        self.api = twitter.Api()

        self.logger = logging.getLogger(__name__)

    def get_names_from_term(self, term):
        """Gets screen names from tweets returned by searching on a term.

        """
        # Get all tweets and process each.
        try:
            self.tweets = self.api.GetSearch(term)
        except Exception as exc:
            raise ServiceError("Couldn't complete Twitter search: {0}".format(exc))
            
        screen_names = []
        for t in self.tweets:
            screen_names.append(t.user.screen_name.encode('utf_8'))

        return screen_names

    def get_tweets_from_name(self, screen_name):
        """Gets tweets from a screen name.

        """
        # Get tweets, or assign a default
        try:
            tweets = self.api.GetUserTimeline(screen_name=screen_name)
        except Exception as exc:
            tweets = ["No tweets found."]

        return tweets

    @staticmethod
    def get_name_from_archive(zip_file_name):
        """Gets screen name from tweet archive.

        """
        # Read user details file within archive zip file
        zip_file = zipfile.ZipFile(zip_file_name, 'r')
        user_details_file = zip_file.open("data/js/user_details.js", 'r')
        user_details_json = re.sub('^.*\{\n', '{', user_details_file.read()).replace('\n', '')
        zip_file.close()

        # Load user details JSON and return screen name
        user_details = json.loads(user_details_json)
        return user_details['screen_name']

    @staticmethod
    def extract_tweets_from_archive(zip_file_name, archive_dir):
        """Extracts JSON tweet files from zip file archive to the
        specified archive directory.

        """
        # Consider each member of the archive
        zip_file = zipfile.ZipFile(zip_file_name, 'r')
        for info_list in zip_file.infolist():

            # Extract JSON data files
            if info_list.filename.find('data/js/tweets') != -1:
                zip_file.extract(info_list, archive_dir)

        zip_file.close()
