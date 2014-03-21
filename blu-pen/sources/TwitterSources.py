# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import ConfigParser
import logging
import math
import os
import pickle
import random
import sys
import time

# Third-party imports
import numpy as np
import twitter

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from authors.BluePeninsulaUtility import BluePeninsulaUtility

class TwitterSources:
    """Represents a collection of Twitter users selected by searching
    for users (the default) or tweets (then looking up users) using a
    query term.

    """
    def __init__(self, config_file, source_word_str,
                 number_of_api_attempts=4, seconds_between_api_attempts=1):
        """Constructs a TwitterSources instance given a source word.

        """
        self.blu_pen_utl = BluePeninsulaUtility()
        
        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.blu_pen_utl.process_source_words(source_word_str)

        # Assign input atributes
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts
        self.content_dir = os.path.join(self.config.get("twitter", "content_dir"), self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        # Initialize created atributes
        self.users = []
        self.name = []
        self.description = []
        self.screen_name = []
        self.created_at = []
        self.statuses_count = []
        self.followers_count = []

        # Create a logger
        self.logger = logging.getLogger(u"TwitterSources")

        # Check input arguments
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))

    def get_users_by_source(self, source_type, source_word, count=20, page=0, max_id=0, since_id=0):
        """Makes multiple attempts to get users by source, sleeping
        before attempts.

        """
        # Initialize return value
        users = []

        def make_attempt():
            """Makes a single attempt to get users by source, sleeping
            before the attempt.

            """
            # Create an API instance with random credentials
            credentials = self.get_credentials()
            api = twitter.Api(consumer_key=credentials['consumer-key'],
                              consumer_secret=credentials['consumer-secret'],
                              access_token_key=credentials['access-token'],
                              access_token_secret=credentials['access-token-secret'])

            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get users by source
            try:

                # Select the API method by source type
                exc = None
                if source_type == "@":
                    if not max_id == 0 or not since_id == 0:
                        raise Exception("A search for users cannot contain a max_id or since_id parameter.")

                    # Use the page parameter, if present
                    if page == 0:
                        users.extend(api.GetUsersSearch(term=source_word, count=count))

                    else:
                        users.extend(api.GetUsersSearch(term=source_word, count=count, page=page))

                else: # source_type == "#":
                    if not page == 0:
                        raise Exception("A search for tweets cannot contain a page parameter.")

                    # Use the page max_id or since_id parameter, if present
                    term = source_type + source_word
                    if max_id == 0 and since_id == 0:
                        tweets = api.GetSearch(term=term, count=count)

                    elif max_id > 0 and since_id == 0:
                        tweets = api.GetSearch(term=term, count=count, max_id=max_id)
                                               
                    elif max_id == 0 and since_id > 0:
                        tweets = api.GetSearch(term=term, count=count, since_id=since_id)
                                               
                    else:
                        raise Exception("A search for tweets cannot contain both a max_id and since_id parameter.")

                    # Lookup users given tweet screen names
                    if len(tweets) > 0:
                        screen_names = self.get_names_from_tweets(tweets)
                        users.extend(api.UsersLookup(screen_name=screen_names))

                self.logger.info(u"{0}: found users for {1}{2}".format(
                    self.source_log, source_type, source_word))

            except Exception as exc:
                self.logger.warning(u"{0}: couldn't find users for {1}{2}: {3}".format(
                        self.source_log, source_type, source_word, exc))

        # Make attempts to get users by source
        iAttempts = 1
        make_attempt()
        while len(users) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return users

    def get_credentials(self):
        """Draw random credentials.

        """
        # Assign OAuth credentials
        oauth = {
            'ep-worker-01': {
                'consumer-key': 'A0yHJTiDt4q9GGfJgJY1A',
                'consumer-secret': 'fY2JtAOTrDXvaSxpVLdPFWUiCGhsRyCiFtDYJRb2E',
                'access-token': '52077242-lKaHnhr3av8gOrdnBqSIOLL0crUE2qh5MAYpxQ0Zq',
                'access-token-secret': '1TLzCUXjmhnOnju4o2hCyS7IfZx0luT4T9LB5Me6r0',
                },
            'ep-worker-02': {
                'consumer-key': 'jLoPJ6YPfLGUfbyitAFnmw',
                'consumer-secret': 'kmzbab9M5FOy324FnkfJcpsFO0DFVWVBpJb9UNmSd4',
                'access-token': '52077242-KlNOJSfnIZBRf2u7UAhxgPc8PmOPDUO6VJUM9vcdt',
                'access-token-secret': 'wt1uC7jw9y7fpdtvXvK7irbZwYgZoEeMiz6U5yCAL1k',
                },
            'ep-worker-03': {
                'consumer-key': '4CZM3ZludoAZ5Na8XpK7g',
                'consumer-secret': 'QHogLTweJBTnr9pnyFmBNb9WHfwQEDV7FPpSkf1E',
                'access-token': '52077242-WW4uDKAPZ3xMmvwN44QryhaHwKg6OyKMRNA5hmxtN',
                'access-token-secret': 'eLBmoPdS2YA9m6K7m0arnuUjXlZAJ6UlTWCm9g',
                },
            'ep-worker-04': {
                'consumer-key': 'TfYDt7N7M1NYW8x96kw',
                'consumer-secret': 'eAKIj0dDS5SnTGp00MBXWbnHecAX4l2GJADIMO4fBE',
                'access-token': '52077242-0v1EIjx1seodvUZJc42OxS1S4dypQOom55yKzZ0dW',
                'access-token-secret': 'fxhR1s0sOdONp63KHD4uBuVC6eo3DwxTuk5RcMgtfg',
                },
            'ep-worker-05': {
                'consumer-key': 'vC3022gsZEY7ggaydP6yBg',
                'consumer-secret': 'aBTK6vBPmNrqhQWkN7zt41RvnqBelwxl3LeSIWC4',
                'access-token': '52077242-dhfzUdhZrN9rSgGCcRUE5IsH5ScYZQI07EKhQho',
                'access-token-secret': '5puUBqcPgXPrRubCDSCxNEAPP7GRZhyukucJDTjyWc',
                },
            'ep-worker-06': {
                'consumer-key': 'IBwcy7R0qbnSFKkFRezNg',
                'consumer-secret': 'sLHTbCILef7dW3encNgJTMDZ6lO2CFofdhKetfBdg',
                'access-token': '52077242-ylaOdeRwFljSX9DPJinXfxGgxrUits9FGOPolHzlI',
                'access-token-secret': 'Kx1IusxAnoE8A3QOheH1KKb047EmBQGZf82GZkndUw',
                },
            'ep-worker-07': {
                'consumer-key': '3xiPySeXWyYSFBhXwTLrWw',
                'consumer-secret': 'KgHruJblAjKQTVI30DhDqLHRGRiyqXcGrKkP5Thvw',
                'access-token': '52077242-Yzgiz7m7YYdP4jRGxgLepC3SfnASwcyBOEcVys3I',
                'access-token-secret': 'Zqn5qwVzHcLQbQktLZY1XKpRjHHsWFtcdogkaWeDA',
                },
            'ep-worker-08': {
                'consumer-key': 'dgfLTwRT95YU3Xf6JY4w',
                'consumer-secret': 'QnU4NH8CcomFoHMRke5KN3Uyb0rFB4shQGN1BGKAM',
                'access-token': '52077242-oWRJqzz2pUIDWbHD06wPqoH2uJeCelFb2PE3b3LcA',
                'access-token-secret': 'NEuxOpDNkDUePG9VAMGIBntUDsGjf9yA97ATHFT9I',
                },
            'ep-worker-09': {
                'consumer-key': 'CNdNL4nlgw7lEwzM09hpw',
                'consumer-secret': 'KeXxmAGrf8aWk157VI5JULM83MOs5Z0PBZZscLsFWow',
                'access-token': '52077242-ZjiT43XPlvbTFxxmbKJzgXMWfGCFGqwp0yf4ZGWWg',
                'access-token-secret': 'yveMVVrHXzKHlRnM7aLonnoCk2bK4XQVJkGax2sdm8',
                },
            'ep-worker-10': {
                'consumer-key': 'YBzJfCTzMhtVmIJGxK4SxQ',
                'consumer-secret': '2WuRfI310GmWxKsb6FDf1eI57RTZiK09S4JwpqfocKs',
                'access-token': '52077242-kP3KoP4dml6R0EVvwRgLsrYhEChrWI2cA3SOt0Zrj',
                'access-token-secret': 'dIflTP4zVFTQWIr1uTZHnnM3JUxQLSMzt6To5lpbg',
                },
            'ep-worker-11': {
                'consumer-key': 'Dwba3YcE4usu3AmSTp0cA',
                'consumer-secret': '08Dfh6ZOcREGOxnzPULM55MLgF4wlHPigf6emr9M70',
                'access-token': '52077242-3jVv44LhPIcXBrQmq4L1jsZ1IwGOUs6TMYsZ2Z7kT',
                'access-token-secret': 'bmWRVDYDuT5ub1R6RlK4oiyWtvnR5nfGaFpZfz8UU',
                },
            'ep-worker-12': {
                'consumer-key': '6h9du9gmGXg8uMvIvlP2CA',
                'consumer-secret': 'VBwI4BrNe8LAOt9mzvxSAdPSKCey3IlMHRMHtA4eZE',
                'access-token': '52077242-Nwos5zC1pZcgtM1d95ZCQAkUafnwaddVjg72rwybK',
                'access-token-secret': 'JHAP5U26aVzJsH0e133aAzvDE64v4XsZB1BysukGY',
                },
            'ep-worker-13': {
                'consumer-key': '7HLZOHKwnXCDZMz9i3hdQ',
                'consumer-secret': 'OX988JqktlxybobRjPrjFlKJyOFihX3D2yFLZ6mluA',
                'access-token': '52077242-tD1YY2AmGr4K845EtBbrzhglj0mgIejDQazbX6c',
                'access-token-secret': 'rqPNMWuBURxHvHxShSba5vzO4wQBYmBOIkNw2KbE3M',
                },
            'ep-worker-14': {
                'consumer-key': 'Tb5mMrTIG0hI4a0OaCHMA',
                'consumer-secret': 'JTO2qgqRN8nyuQmF6BiLSGIvfagcsSNN6cbMJ1NWSSI',
                'access-token': '52077242-uSBF34RYu9w6lJkVwSf8s58RAdwGBoUN7MBgHqm0A',
                'access-token-secret': 'hYlLo2QUlio4NlHaI5aPUgq3ekte5IZ9mBtawvw0xY',
                },
            'ep-worker-15': {
                'consumer-key': 'fcGOQfFHae1u8S2C82JxTg',
                'consumer-secret': '5pZKNE6xRo4pOwX6Lxth5eg38fJbmPwP0Vhxp31WW4',
                'access-token': '52077242-RxsQK8ri3KzXm6FRUPwdy8UnNDdbEB8zbGyBdWrMY',
                'access-token-secret': '21UEt4tMMpXKuypYAIBSLEhqSMgzEhtC23axspcfAU',
                },
            'ep-worker-16': {
                'consumer-key': 's7w96SOx39B8wkNuO3dEUg',
                'consumer-secret': 'tTNooqoBqJzP6XttSWi2BrUR7RWSFaOThQuDSSKPpA',
                'access-token': '52077242-5qbjb1h7fRwRTtnzz4DVtQ2HG42fdZvmNdkPx1Q7u',
                'access-token-secret': '4b1fw7UIKvFMBET8dRZLQq27ffws5SbzUF6c56r0U0',
                },
            }

        # Draw a random set of credentials
        credentials = oauth[random.sample(oauth, 1)[0]]

        return credentials

    def get_names_from_tweets(self, tweets):
        """Creates a list of screen names given an array of tweets.
        
        """
        screen_names = []

        for t in tweets:
            screen_names.append(t.user.screen_name)

        return screen_names

    def set_sources(self):
        """Create and dump, or load, the TwitterSources pickle.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Create and dump, or load, the TwitterSources pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0}: finding sources using {1}".format(
                self.source_log, source_word_str))

            # Select one hundred users
            for page in range(1, 6):
                self.users.extend(self.get_users_by_source(self.source_type, self.source_word, page=page))
            
            # Assign arrays of values for selecting users
            for u in self.users:
                self.name.append(u.name)
                self.description.append(u.description)
                self.screen_name.append(u.screen_name)
                self.created_at.append(u.created_at)
                self.statuses_count.append(u.statuses_count)
                self.followers_count.append(u.followers_count)

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def n_to_s(self, scores):
        """Converts a numerical score to either a "-" if below the
        median, a "+" if above the median, or a "~" otherwise.

        """
        threshold = np.median(scores)
        strings = []
        for score in scores:
            if score < threshold:
                string = "-"
            elif score > threshold:
                string = "+"
            else:
                string = "~"
            strings.append(string)
        return np.array(strings)

    def dump(self, pickle_file_name=None):
        """Dumps TwitterSources attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_log'] = self.source_log
        p['source_path'] = self.source_path
        p['source_header'] = self.source_header
        p['source_label'] = self.source_label
        p['source_type'] = self.source_type
        p['source_word'] = self.source_word

        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['users'] = self.users

        p['name'] = self.name
        p['description'] = self.description
        p['screen_name'] = self.screen_name
        p['created_at'] = self.created_at
        p['statuses_count'] = self.statuses_count
        p['followers_count'] = self.followers_count

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} users to {2}".format(
            self.source_log, len(self.users), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads TwitterSources attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_log = p['source_log']
        self.source_path = p['source_path']
        self.source_header = p['source_header']
        self.source_label = p['source_label']
        self.source_type = p['source_type']
        self.source_word = p['source_word']

        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.users = p['users']

        self.name = p['name']
        self.description = p['description']
        self.screen_name = p['screen_name']
        self.created_at = p['created_at']
        self.statuses_count = p['statuses_count']
        self.followers_count = p['followers_count']

        self.logger.info(u"{0} loaded {1} users from {2}".format(
            self.source_log, len(self.users), pickle_file_name))

        pickle_file.close()
