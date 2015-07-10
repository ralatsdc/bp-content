# -*- coding: utf-8 -*-

# Standard library imports
import json
import logging
import os
import random
import re
import sys
import zipfile

# Third-party imports
import twitter

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utility.ServiceError import ServiceError

class TwitterUtility(object):
    """Provides utilities for using Twitter.

    """
    def __init__(self):
        """Constructs a TwitterUtility.

        """
        credentials = self.get_credentials()
        self.api = twitter.Api(
            consumer_key=credentials['consumer-key'],
            consumer_secret=credentials['consumer-secret'],
            access_token_key=credentials['access-token'],
            access_token_secret=credentials['access-token-secret'])

        self.logger = logging.getLogger(__name__)

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

    def get_names_from_term(self, term, search="tweets"):
        """Gets screen names from tweets returned by searching on a term.

        """
        # Get all tweets and process each.
        try:
            if search == "tweets":
                tweets = self.api.GetSearch(term)

            elif search == "users":
                users = self.api.GetUsersSearch(term)

            else:
                raise Exception("Unknown search type: {0}".format(search))

        except Exception as exc:
            raise ServiceError("Couldn't complete Twitter search: {0}".format(exc))
            
        screen_names = []

        if search == "tweets":
            for t in tweets:
                screen_names.append(t.user.screen_name.encode('utf_8'))

        elif search == "users":
            for u in users:
                screen_names.append(u.screen_name.encode('utf_8'))
                
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
