# -*- coding: utf-8 -*-

# Standard library imports
import datetime
import glob
import logging
import math
import os
import pickle
import random
import re
import simplejson as json
import time

# Third-party imports
import numpy as np
import twitter
import webcolors

# Local imports
from utility.AuthorUtility import AuthorUtility

class TwitterAuthor(object):
    """Represents author on Twitter by their creative output. Author
    is selected by name or tag.

    """
    def __init__(self, blu_pen_author, source_words_str, content_dir,
                 start_date=datetime.date(2006, 07, 15) - datetime.timedelta(2),
                 stop_date=datetime.date.today() + datetime.timedelta(2),
                 max_length=100, number_of_api_attempts=4, seconds_between_api_attempts=1):
        """Constructs a TwitterAuthor instance given source words.

        """
        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.blu_pen_author = blu_pen_author
        self.author_utility = AuthorUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_types,
         self.source_words) = self.author_utility.process_source_words(source_words_str)
        if not isinstance(self.source_types, list):
            self.source_types = [self.source_types]
        if not isinstance(self.source_words, list):
            self.source_words = [self.source_words]
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")
        self.start_date = start_date
        self.stop_date = stop_date
        self.twitter_start_date = datetime.date(2006, 07, 15) - datetime.timedelta(2)
        self.twitter_stop_date = datetime.date.today() + datetime.timedelta(2)
        self.max_length = max_length / len(self.source_words) # per source word
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        # Initialize created attributes
        self.page = [0] * len(self.source_words)
        self.max_id = [0] * len(self.source_words)
        self.since_id = [0] * len(self.source_words)

        self.last_tweets = {}

        self.tweets = set()

        self.tweet_id = []
        self.created_dt = []
        self.text_symbol = []
        self.clean_text = []
        self.sidebar_fill_rgb = []
        self.link_rgb = []
        self.text_rgb = []
        self.background_rgb = []

        self.length = np.zeros((len(self.source_words), 1))
        self.count = np.zeros((len(self.source_words), 1))
        self.volume = np.zeros((24, 12))
        self.frequency = {}

        self.content_set = False

        self.logger = logging.getLogger(__name__)

    def set_tweets(self, count=100, parameter="max_id", do_purge=False):
        """Gets recents tweets from Twitter for all source words. Note
        that the default Twitter API behavior is to provide recent
        tweets first. Then additional tweets may be obtained by paging
        using a request for tweets having a tweet identifier less than
        a maximum tweet identifier, or have a tweet identifier greater
        than a since tweet identifier.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the TwitterAuthor pickle
        if not os.path.exists(self.pickle_file_name):

            # Load attributes pickle, if it exists, and initialize loop
            # variables
            if os.path.exists(self.pickle_file_name):
                self.load()

            n_before_twitter_start_tot = 0 # Before twitter start date
            n_after_twitter_stop_tot = 0 # After twitter stop date
            n_after_stop_tot = 0 # After stop date
            n_convert_exceptions_tot = 0 # With convert exceptions
            n_flash_objects_tot = 0 # With flash objects
            n_duplicate_texts_tot = 0 # With duplicate texts

            # Get one page of tweets for each source word in turn until
            # all tweets for each source word have been gotten (defined as
            # a tweet before the start date, or more dates than the
            # maximum length)
            nSrc = len(self.source_words)
            do_get_source_content = [True] * nSrc
            while True in do_get_source_content:
                for iSrc in range(nSrc):
                    # Skip source words for which all tweets have been
                    # gotten
                    if not do_get_source_content[iSrc]:
                        continue
                    self.logger.info(u"{0} getting content for {1}{2}".format(
                            self.source_log, self.source_types[iSrc], self.source_words[iSrc]))
                    # Note that the final request must return no
                    # tweets, otherwise, this while loop never terminates.
                    # Note also that the page parameter, though valid, is
                    # no longer used in API requests.
                    self.page[iSrc] += 1
                    try:
                        if parameter == "max_id":
                            # Note that since the max_id parameter is
                            # inclusive, it is reduced by one so the Tweet
                            # with the matching identifier will not be
                            # returned again.
                            tweets = self.get_tweets_by_source(
                                self.source_types[iSrc], self.source_words[iSrc],
                                count=count, page=self.page[iSrc], max_id=self.max_id[iSrc] - 1)
                        elif parameter == "since_id":
                            # Note that since the since_id parameter is
                            # not inclusive, it is unchanged and the Tweet
                            # with the matching identifier will not be
                            # returned again.
                            tweets = self.get_tweets_by_source(
                                self.source_types[iSrc], self.source_words[iSrc],
                                count=count, page=self.page[iSrc], since_id=self.since_id[iSrc])
                        if len(tweets) == 0 or tweets == self.last_tweets:
                            if self.length[iSrc] > 0:
                                self.logger.debug(u"{0} =0= tweets found".format(
                                    self.source_log))
                                do_get_source_content[iSrc] = False
                                continue
                            else:
                                # No tweets found, so raise an exception to catch below
                                self.logger.warning(u"{0} =1= no tweets found".format(
                                    self.source_log))
                                raise Exception("No tweets found")
                        self.logger.debug(
                            u"{0} got page {1} of {2} before {3} or after {4} for {5}{6} containing {7} tweets".format(
                            self.source_log, self.page[iSrc], count, self.max_id[iSrc], self.since_id[iSrc],
                            self.source_types[iSrc], self.source_words[iSrc], len(tweets)))
                        self.last_tweets = tweets
                    except Exception as exc:
                        message = str(exc)
                        if message.find("No tweets found") != -1:
                            # No tweets found, so catch the exception raised
                            # above, and raise the exception again so that the
                            # task can fail immediately, if considering
                            # the last source word
                            self.logger.warning(u"{0} =2= no tweets found".format(
                                self.source_log))
                            if iSrc == nSrc - 1:
                                raise Exception("No tweets found")
                        elif message.find("Rate limit exceeded") != -1:
                            # Rate limit exceeded, so catch the exception
                            # raised by the Twitter API, and raise the
                            # exception again so that the task can handle
                            # it with the correct (very long) delay
                            self.logger.warning(u"{0} =3= rate limit exceeded".format(
                                self.source_log))
                            raise Exception("Rate limit exceeded")
                        elif message.find("Capacity Error") != -1:
                            # Capacity exceeded, so catch the exception
                            # raised by the Twitter API, and raise the
                            # exception again so that the task can handle
                            # it with the correct (somewhat long) delay
                            self.logger.warning(u"{0} =4= Capacity exceeded".format(
                                self.source_log))
                            raise Exception("Capacity exceeded")
                        else:
                            if self.length[iSrc] > 0:
                                self.logger.debug(u"{0} =5= tweets found".format(
                                    self.source_log))
                                do_get_source_content[iSrc] = False
                                continue
                            else:
                                # Try getting the timeline for a known user
                                try:
                                    source_type = u'@'
                                    source_word = "BluePeninsula"
                                    tweets = self.get_tweets_by_source(
                                        source_type, source_word, count=count)
                                    self.logger.debug(u"{0} got {1} tweets for {2}{3}".format(
                                            self.source_log, len(tweets), source_type, source_word))
                                except Exception as exc:
                                    # Problem with twitter, so raise an
                                    # exception so that the task can handle it
                                    # with the correct (short) delay
                                    self.logger.warning(u"{0} =6= problem with twitter for {1}{2}: {3}".format(
                                            self.source_log, source_type, source_word, exc))
                                    raise Exception("Problem with twitter")
                                # No tweets found, so raise an exception
                                # so that the task can fail immediately,
                                # if considering the last source word
                                self.logger.warning(u"{0} =7= no tweets found".format(
                                    self.source_log))
                                if iSrc == nSrc - 1:
                                    raise Exception("No tweets found")

                    # Process each tweet in reverse chronological order
                    n_before_twitter_start_cur = 0 # Before twitter start date
                    n_after_twitter_stop_cur = 0 # After twitter stop date
                    n_after_stop_cur = 0 # After stop date
                    n_convert_exceptions_cur = 0 # With convert exceptions
                    n_flash_objects_cur = 0 # With flash objects
                    n_duplicate_texts_cur = 0 # With duplicate texts
                    def convert_string_datetime(tweet):
                        if self.source_types[iSrc] == u'@':
                            tweet_dt = datetime.datetime.strptime(tweet.created_at, "%a %b %d %H:%M:%S +%f %Y")
                        else: # self.source_types[iSrc] = u'#':
                            tweet_dt = datetime.datetime.strptime(tweet.created_at, "%a, %d %b %Y %H:%M:%S +%f")
                        return tweet_dt
                    for tweet in sorted(tweets, key=convert_string_datetime, reverse=True):

                        # Skip the current tweet if it is after the
                        # twitter stop date, before the twitter start
                        # date, or after the stop date. Skip all remaining
                        # tweets if the current tweet is before the start
                        # date, or if the maximum number of tweets have
                        # been collected.
                        if self.source_types[iSrc] == u'@':
                            tweet_dt = datetime.datetime.strptime(tweet.created_at, "%a %b %d %H:%M:%S +%f %Y")
                        else: # self.source_types[iSrc] = u'#':
                            tweet_dt = datetime.datetime.strptime(tweet.created_at, "%a, %d %b %Y %H:%M:%S +%f")
                        if datetime.date(tweet_dt.year, tweet_dt.month, tweet_dt.day) > self.twitter_stop_date:
                            self.logger.debug(u"{0} continuing: {1} after twitter stop date {2}".format(
                                    self.source_log, tweet_dt, self.twitter_stop_date))
                            n_after_twitter_stop_cur += 1
                            n_after_twitter_stop_tot += 1
                            continue
                        if datetime.date(tweet_dt.year, tweet_dt.month, tweet_dt.day) < self.twitter_start_date:
                            self.logger.debug(u"{0} continuing: {1} before twitter start date {2}".format(
                                    self.source_log, tweet_dt, self.twitter_start_date))
                            n_before_twitter_start_cur += 1
                            n_before_twitter_start_tot += 1
                            continue
                        if datetime.date(tweet_dt.year, tweet_dt.month, tweet_dt.day) > self.stop_date:
                            self.logger.debug(u"{0} continuing: {1} after stop date {2}".format(
                                    self.source_log, tweet_dt, self.stop_date))
                            n_after_stop_cur += 1
                            n_after_stop_tot += 1
                            continue
                        if datetime.date(tweet_dt.year, tweet_dt.month, tweet_dt.day) < self.start_date:
                            self.logger.debug(u"{0} continuing: {1} before start date {2}".format(
                                    self.source_log, tweet_dt, self.start_date))
                            do_get_source_content[iSrc] = False
                            continue
                        if self.length[iSrc] == self.max_length:
                            self.logger.debug(u"{0} breaking: {1} exceeded max length {2}".format(
                                    self.source_log, tweet_dt, self.max_length))
                            do_get_source_content[iSrc] = False
                            break

                        # Convert HTML entities
                        # try:
                        #     text = lxml.html.fromstring(tweet.text).text_content()
                        # except Exception as exc:
                        #     self.logger.error(u"{0} continuing: couldn't convert HTML entities in {1}: {2}".format(
                        #             self.source_log, tweet.text, exc))
                        #     n_convert_exceptions_cur += 1
                        #     n_convert_exceptions_tot += 1
                        #     continue

                        # Remove the unicode replacement character
                        # text = text.replace(u'\ufffd', '')

                        # Select the encoding
                        # text = text.encode('utf_8')

                        # Watch for flash
                        # if text.find("registerObject") != -1:
                        #     self.logger.warning(u"{0} continuing: contains flash object".format(
                        #         self.source_log))
                        #     n_flash_objects_cur += 1
                        #     n_flash_objects_tot += 1
                        #     continue

                        # Skip the current tweet, if it has already been
                        # processed
                        if tweet.id in self.tweet_id:
                            self.logger.debug(u"{0} continuing: duplicate tweet text {1}".format(
                                self.source_log, tweet.text))
                            n_duplicate_texts_cur += 1
                            n_duplicate_texts_tot += 1
                            continue
                        else:
                            self.tweet_id.append(tweet.id)
                            if self.max_id[iSrc] == 0 or self.max_id[iSrc] == -1 or tweet.id < self.max_id[iSrc]:
                                self.max_id[iSrc] = tweet.id
                            if self.since_id[iSrc] == 0 or tweet.id > self.since_id[iSrc]:
                                self.since_id[iSrc] = tweet.id

                        # Add and count this tweet
                        self.tweets.add(tweet)
                        self.length[iSrc] += 1
                        if self.count[iSrc] < tweet.user.statuses_count:
                            self.count[iSrc] = tweet.user.statuses_count

                        # Convert and append create date and time
                        if self.source_types[iSrc] == u'@':
                            self.created_dt.append(datetime.datetime.strptime(tweet.created_at, "%a %b %d %H:%M:%S +%f %Y"))
                        else: # self.source_types[iSrc] = u'#':
                            self.created_dt.append(datetime.datetime.strptime(tweet.created_at, "%a, %d %b %Y %H:%M:%S +%f"))

                        # Append text symbol
                        if nSrc == 1:
                            self.text_symbol.append("bullet")
                        else:
                            if self.source_types[iSrc] == u'@':
                                found_all = False
                            else:
                                found_all = True
                                for jSrc in range(nSrc):
                                    if tweet.text.lower().find(self.source_words[jSrc].lower()) == -1:
                                        found_all = False
                                        break
                            # TODO: Use more symbols.
                            if found_all:
                                self.text_symbol.append("bullet")
                            elif iSrc == 0:
                                self.text_symbol.append("clubsuit")
                            else:
                                self.text_symbol.append("spadesuit")

                        # Append clean text
                        self.clean_text.append(tweet.text)

                        # Convert color from hex to RGB
                        try:
                            sidebar_fill_color = webcolors.hex_to_rgb(
                                u'#' + tweet.user.profile_sidebar_fill_color)
                        except Exception as exc:
                            sidebar_fill_color = webcolors.hex_to_rgb("#000000")
                        try:
                            link_color = webcolors.hex_to_rgb(u'#' + tweet.user.profile_link_color)
                        except Exception as exc:
                            link_color = webcolors.hex_to_rgb("#000000")
                        try:
                            text_color = webcolors.hex_to_rgb(u'#' + tweet.user.profile_text_color)
                        except Exception as exc:
                            text_color = webcolors.hex_to_rgb("#000000")
                        try:
                            background_color = webcolors.hex_to_rgb(
                                u'#' + tweet.user.profile_background_color)
                        except Exception as exc:
                            background_color = webcolors.hex_to_rgb("#000000")

                        # Scale RGB values
                        self.sidebar_fill_rgb.append([c / 255.0 for c in sidebar_fill_color])
                        self.link_rgb.append([c / 255.0 for c in link_color])
                        self.text_rgb.append([c / 255.0 for c in text_color])
                        self.background_rgb.append([c / 255.0 for c in background_color])

                        # Increment tweet volume
                        # Twenty-four hour clock is zero based
                        twt_hour = int(self.created_dt[-1].strftime("%H"))
                        # Months are one based
                        twt_month = int(self.created_dt[-1].strftime("%m")) - 1
                        self.volume[twt_hour, twt_month] += 1
                        
                    # Dump attributes pickle after each tweet page
                    self.dump()
                    self.logger.debug(u"{0} found {1} tweets before twitter start date {2}".format(
                            self.source_log, n_before_twitter_start_cur, self.twitter_start_date))
                    self.logger.debug(u"{0} found {1} tweets after twitter stop date {2}".format(
                            self.source_log, n_after_twitter_stop_cur, self.twitter_stop_date))
                    self.logger.debug(u"{0} found {1} tweets after stop date {2}".format(
                            self.source_log, n_after_stop_cur, self.stop_date))
                    self.logger.debug(u"{0} found {1} tweets with convert exceptions".format(
                        self.source_log, n_convert_exceptions_cur))
                    self.logger.debug(u"{0} found {1} tweets with flash objects".format(
                        self.source_log, n_flash_objects_cur))
                    self.logger.debug(u"{0} found {1} tweets with duplicate texts".format(
                        self.source_log, n_duplicate_texts_cur))

                # Dump attributes pickle after each source word
                self.dump()
                self.logger.info(u"{0} found {1} tweets before twitter start date {2}".format(
                        self.source_log, n_before_twitter_start_tot, self.twitter_start_date))
                self.logger.info(u"{0} found {1} tweets after twitter stop date {2}".format(
                        self.source_log, n_after_twitter_stop_tot, self.twitter_stop_date))
                self.logger.info(u"{0} found {1} tweets after stop date {2}".format(
                        self.source_log, n_after_stop_tot, self.stop_date))
                self.logger.info(u"{0} found {1} tweets with convert exceptions".format(
                    self.source_log, n_convert_exceptions_tot))
                self.logger.info(u"{0} found {1} tweets with flash objects".format(
                    self.source_log, n_flash_objects_tot))
                self.logger.info(u"{0} found {1} tweets with duplicate texts".format(
                    self.source_log, n_duplicate_texts_tot))

            # Compare length of found tweets to length of expected tweets
            length_fnd = len(self.clean_text)
            length_exp = int(min(self.count.sum(), self.max_length * len(self.source_words)))
            if (length_fnd < length_exp):
                self.logger.info(u"{0} found {1} tweets is less than expected {2} tweets by {3} tweet(s)".format(
                    self.source_log, length_fnd, length_exp, length_exp - length_fnd))

            # Sort tweet values chronologically
            self.sort_tweets()

            # Compute word frequency
            self.compute_word_frequency()

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_tweets_by_source(self, source_type, source_word, count=0, page=0, max_id=0, since_id=0):
        """Makes multiple attempts to get source content, sleeping
        before attempts.

        """
        tweets = None

        # Make multiple attempts to get source content
        iAttempts = 0
        while tweets is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Draw a set of random credentials and create a
            # corresponding API instance
            credentials = self.get_credentials()
            api = twitter.Api(
                consumer_key=credentials['consumer-key'],
                consumer_secret=credentials['consumer-secret'],
                access_token_key=credentials['access-token'],
                access_token_secret=credentials['access-token-secret'])

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} =8= sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make attempt to get source content
            try:
                if source_type == u'@':
                    if not max_id > 0 and not since_id > 0:
                        tweets = api.GetUserTimeline(screen_name=source_word,
                                                     count=count,
                                                     include_rts=True)
                    elif max_id > 0 and not since_id > 0:
                        tweets = api.GetUserTimeline(screen_name=source_word,
                                                     count=count, max_id=max_id,
                                                     include_rts=True)
                    elif not max_id > 0 and since_id > 0:
                        tweets = api.GetUserTimeline(screen_name=source_word,
                                                     count=count, since_id=since_id,
                                                     include_rts=True)
                    else:
                        raise Exception("A request should not use both max_id and since_id.")

                else: # source_type == u'#':
                    term = source_type + source_word
                    if not max_id > 0 and not since_id > 0:
                        tweets = api.GetSearch(term=term,
                                               count=count)
                    elif max_id > 0 and not since_id > 0:
                        tweets = api.GetSearch(term=term,
                                               count=count, max_id=max_id)
                    elif not max_id > 0 and since_id > 0:
                        tweets = api.GetSearch(term=term,
                                               count=count, since_id=since_id)
                    else:
                        raise Exception("A request should not use both max_id and since_id.")

            except Exception as exc:
                tweets = None
                self.logger.warning(u"{0} =9= couldn't get content for {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        return tweets

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

    def set_tweets_from_archive(self, do_purge=False):
        """Gets content from JSON tweet data files extracted from a
        Twitter zip file archive.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the TwitterAuthor pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0} getting tweets for {1}{2}".format(
                self.source_log, self.source_types, self.source_words))

            # Process each JSON file
            json_file_names = glob.glob(self.content_dir + "/data/js/tweets/*.js")
            for json_file_name in json_file_names:

                # Read current JSON file into a string
                json_file = open(json_file_name, 'r')
                json_str = json_file.read()
                json_file.close()

                # Remove leading assignment statement
                json_str = re.sub('^.*\n.*\[', '[', json_str)

                # Load and process tweets
                try:
                    tweets = json.loads(json_str)
                except Exception as exc:
                    # TODO: Add logging message.
                    continue
                for tweet in tweets:

                    # Convert HTML entities
                    # try:
                    #     text = lxml.html.fromstring(tweet['text']).text_content()
                    # except Exception as exc:
                    #     self.logger.error(u"{0} continuing: couldn't convert HTML entities in {1}: {2}".format(
                    #             self.source_log, tweet['text'], exc))
                    #     n_convert_exceptions += 1
                    #     continue

                    # Remove the unicode replacement character
                    # text = text.replace(u'\ufffd', '')

                    # Select the encoding
                    # text = text.encode('utf_8')

                    # Watch for flash
                    # if text.find("registerObject") != -1:
                    #     self.logger.warning(u"{0} continuing: contains flash object".format(
                    #         self.source_log))
                    #     n_flash_objects += 1
                    #     continue

                    # Skip the current tweet, if it has already been
                    # processed
                    # if tweet['id'] in self.tweet_id:
                    #     self.logger.debug(u"{0} continuing: duplicate tweet text {1}".format(
                    #         self.source_log, tweet.text))
                    #     n_duplicate_texts += 1
                    #     continue
                    # else:
                    #     self.tweet_id.append(tweet['id'])

                    # Add and count this tweet
                    self.tweets.add(str(tweet))
                    self.length[0] += 1

                    # Convert and append create date and time
                    self.created_dt.append(
                        datetime.datetime.strptime(
                            tweet['created_at'][0:19], "%Y-%m-%d %H:%M:%S"))

                    # Append text symbol
                    self.text_symbol.append("bullet")

                    # Append clean text
                    self.clean_text.append(tweet.text)

                    # Convert color from hex to RGB
                    sidebar_fill_color = webcolors.hex_to_rgb("#000000")
                    link_color = webcolors.hex_to_rgb("#000000")
                    text_color = webcolors.hex_to_rgb("#000000")
                    background_color = webcolors.hex_to_rgb("#000000")

                    # Scale RGB values
                    self.sidebar_fill_rgb.append([c / 255.0 for c in sidebar_fill_color])
                    self.link_rgb.append([c / 255.0 for c in link_color])
                    self.text_rgb.append([c / 255.0 for c in text_color])
                    self.background_rgb.append([c / 255.0 for c in background_color])

                    # Increment tweet volume
                    # Twenty-four hour clock is zero based
                    twt_hour = int(self.created_dt[-1].strftime("%H"))
                    # Months are one based
                    twt_month = int(self.created_dt[-1].strftime("%m")) - 1
                    self.volume[twt_hour, twt_month] += 1

            # Sort tweet values chronologically
            self.sort_tweets()

            # Compute word frequency
            self.compute_word_frequency()

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def sort_tweets(self):
        """Sort tweet values chronologically.

        """
        tmp = zip(self.tweet_id, self.created_dt, self.text_symbol, self.clean_text,
                  self.sidebar_fill_rgb, self.link_rgb, self.text_rgb, self.background_rgb)
        tmp.sort()
        (tweet_id, created_dt, text_symbol, clean_text,
         sidebar_fill_rgb, link_rgb, text_rgb, background_rgb) = zip(*tmp)
        self.tweet_id = list(tweet_id)
        self.created_dt = list(created_dt)
        self.text_symbol = list(text_symbol)
        self.clean_text = list(clean_text)
        self.sidebar_fill_rgb = list(sidebar_fill_rgb)
        self.link_rgb = list(link_rgb)
        self.text_rgb = list(text_rgb)
        self.background_rgb = list(background_rgb)
        self.logger.info(u"{0} set {1} tweets".format(
            self.source_log, len(self.clean_text)))

    def compute_word_frequency(self):
        """Counts the number of times a word appears.

        """
        p_starts_with = re.compile("^[a-zA-Z@#]")
        p_contains = re.compile("[a-zA-Z0-9@#&_]+")
        for text in self.clean_text:
            words = text.replace('@', ' @').replace('#', ' #').split()
            for word in words:
                m_starts_with = p_starts_with.match(word)
                m_contains = p_contains.match(word)
                if (m_starts_with
                    and m_contains and len(m_contains.group()) == len(word)
                    and len(word)) > 1:
                    if word in self.frequency:
                        self.frequency[word] += 1
                    else:
                        self.frequency[word] = 1

    def dump(self, pickle_file_name=None):
        """Dumps TwitterAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['page'] = self.page
        p['max_id'] = self.max_id
        p['since_id'] = self.since_id

        p['tweets'] = self.tweets

        p['tweet_id'] = self.tweet_id
        p['created_dt'] = self.created_dt
        p['text_symbol'] = self.text_symbol
        p['clean_text'] = self.clean_text
        p['sidebar_fill_rgb'] = self.sidebar_fill_rgb
        p['link_rgb'] = self.link_rgb
        p['text_rgb'] = self.text_rgb
        p['background_rgb'] = self.background_rgb

        p['length'] = self.length
        p['count'] = self.count
        p['volume'] = self.volume
        p['frequency'] = self.frequency

        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} tweets to {2}".format(
                self.source_log, len(self.clean_text), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads TwitterAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.page = p['page']
        self.max_id = p['max_id']
        self.since_id = p['since_id']

        self.tweets = p['tweets']

        self.tweet_id = p['tweet_id']
        self.created_dt = p['created_dt']
        self.text_symbol = p['text_symbol']
        self.clean_text = p['clean_text']
        self.sidebar_fill_rgb = p['sidebar_fill_rgb']
        self.link_rgb = p['link_rgb']
        self.text_rgb = p['text_rgb']
        self.background_rgb = p['background_rgb']

        self.length = p['length']
        self.count = p['count']
        self.volume = p['volume']
        self.frequency = p['frequency']

        self.content_set = p['content_set']

        self.logger.info(u"{0} loaded {1} tweets from {2}".format(
                self.source_log, len(self.clean_text), pickle_file_name))

        pickle_file.close()
