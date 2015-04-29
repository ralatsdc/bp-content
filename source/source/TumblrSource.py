# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import logging
import math
import os
import pickle
import sys
import time

# Third-party imports
import numpy as np
import pytumblr

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utility.AuthorUtility import AuthorUtility

class TumblrSource(object):
    """Represents a collection of Tumblr blogs selected by getting
    tagged posts.

    """
    def __init__(self, blu_pen_source, source_word_str, content_dir,
                 consumer_key="7c3XQwWIUJS9hjJ9EPzhx2qlySQ5J2sIRgXRN89Ld03AGtK1KP",
                 secret_key="R8Y1Qj7wODcorDid3A24Ct1bfUg0wGoT9iB4n2GgXwKcTb6csb",
                 number_of_api_attempts=8, seconds_between_api_attempts=0.1):
        """Constructs a TumblrSource instance given a configuration
        file and source word.

        """
        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.blu_pen_source = blu_pen_source
        self.author_utility = AuthorUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.author_utility.process_source_words(source_word_str, default_type=u'#')

        # Assign input atributes
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")
        self.consumer_key = consumer_key
        self.secret_key = secret_key
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        # Initialize created attributes
        self.host_names = set()
        self.blog_posts = []

        # Create a client
        self.client = pytumblr.TumblrRestClient(self.consumer_key, self.secret_key)

        # Create a logger
        self.logger = logging.getLogger(u"TumblrSource")

        # Check input arguments
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))
        if not self.source_type == u'#':
            err_msg = u"{0} can only search for posts with tag (#)".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))

    def set_source(self, max_n_host_names=100, do_purge=False):
        """Create and dump, or load, the TumblrSource pickle.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the TumblrSource pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0}: finding source using {1}".format(
                self.source_log, self.source_type + self.source_word))

            # Get blog names by posts with tag
            ns = self.get_blog_names_by_posts_with_tag(self.source_type, self.source_word)
            self.host_names = self.host_names.union(ns['host_names'])
            n_host_names = len(self.host_names)
            additional_names_found = True
            self.logger.info(u"{0}: found {1} unique blog host names using {2}".format(
                self.source_log, n_host_names, self.source_type + self.source_word))
            while additional_names_found and n_host_names < max_n_host_names:
                ns = self.get_blog_names_by_posts_with_tag(self.source_type, self.source_word, before=ns['time_stamp'])
                self.host_names = self.host_names.union(ns['host_names'])
                if len(self.host_names) > n_host_names:
                    additional_names_found = True
                else:
                    additional_names_found = False
                n_host_names = len(self.host_names)
                self.logger.info(u"{0}: found {1} unique blog host names using {2}".format(
                    self.source_log, n_host_names, self.source_type + self.source_word))

            # Get information, total number of posts, and text posts
            # for each blog
            for host_name in self.host_names:
                b_p = self.get_blog_posts_by_hostname(host_name)
                self.blog_posts.append(b_p)

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_blog_names_by_posts_with_tag(self, source_type, source_word, limit=20, before=0):
        """Makes multiple attempts to get blog host names by posts with
        tag, sleeping before attempts.

        """
        host_names = None
        time_stamp = None

        # Make multiple attempts to get blog host names by posts with tag
        iAttempts = 0
        while host_names is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Use the before parameter, if present
            if before == 0:
                posts = self.client.tagged(tag=source_word, limit=limit)

            else:
                posts = self.client.tagged(tag=source_word, limit=limit, before=before)

            # Make an attempt to get blog host names by posts with tag
            try:

                # Use the before parameter, if present
                if before == 0:
                    posts = self.client.tagged(tag=unicode(source_word).encode('utf-8'), limit=limit)

                else:
                    posts = self.client.tagged(tag=unicode(source_word).encode('utf-8'), limit=limit, before=before)

                # Collect unique blog names from posts
                host_names = set()
                time_stamp = [float('inf')]
                for post in posts:
                    host_names.add(post['blog_name'])
                    if post['timestamp'] < time_stamp[0]:
                        time_stamp[0] = post['timestamp']

                self.logger.info(u"{0}: found {1} blog host names by posts with {2}{3}".format(
                    self.source_log, len(host_names), source_type, source_word))

            except Exception as exc:
                host_names = None
                time_stamp = None
                self.logger.warning(u"{0}: couldn't find blog host names by posts with {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        return {'host_names': host_names, 'time_stamp': time_stamp[0]}

    def get_blog_info_by_hostname(self, hostname):
        """Makes multiple attempts to get blog info by hostname,
        sleeping before attempts.

        """
        info = None

        # Make multiple attempts to get blog info by hostname
        iAttempts = 0
        while info is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get blog info by hostname
            try:
                info = self.client.blog_info(hostname)['blog']
                self.logger.info(u"{0}: found blog info for {1}".format(
                    self.source_log, hostname))

            except Exception as exc:
                info = None
                self.logger.warning(u"{0}: couldn't find blog info for {1}: {2}".format(
                    self.source_log, hostname, exc))

        return info

    def get_blog_posts_by_hostname(self, hostname, limit=20, ptype="", offset=0):
        """Makes multiple attempts to get blog posts by hostname,
        sleeping before attempts.

        """
        b_p = None

        # Make multiple attempts to get blog posts by hostname
        iAttempts = 0
        while b_p is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get blog posts by hostname
            try:
                if ptype == "" and offset == 0:
                    b_p = self.client.posts(hostname, limit=limit, notes_info=True)

                elif not ptype == "" and offset == 0:
                    b_p = self.client.posts(hostname, limit=limit, type=ptype, notes_info=True)

                elif ptype == "" and offset > 0:
                    b_p = self.client.posts(hostname, limit=limit, offset=offset, notes_info=True)

                else:
                    b_p = self.client.posts(hostname, limit=limit, type=ptype, offset=offset, notes_info=True)

                self.logger.info(u"{0}: found blog posts for {1}".format(
                    self.source_log, hostname))

            except Exception as exc:
                b_p = None
                self.logger.warning(u"{0}: couldn't find blog posts for {1}: {2}".format(
                    self.source_log, hostname, exc))

        return b_p

    def n_to_s(self, scores):
        """Converts a numerical score to either a "-" if below the
        median, a "+" if above the median, or a "~" otherwise.

        """
        if scores.size == 0:
            return np.empty(scores.shape, np.str)
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
        """Dumps TumblrSource attributes pickle.

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

        p['consumer_key'] = self.consumer_key
        p['secret_key'] = self.secret_key
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['host_names'] = self.host_names
        p['blog_posts'] = self.blog_posts

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} blogs to {2}".format(
            self.source_log, len(self.host_names), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads TumblrSource attributes pickle.

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

        self.consumer_key = p['consumer_key']
        self.secret_key = p['secret_key']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.host_names = p['host_names']
        self.blog_posts = p['blog_posts']

        self.logger.info(u"{0} loaded {1} blogs from {2}".format(
            self.source_log, len(self.host_names), pickle_file_name))

        pickle_file.close()
