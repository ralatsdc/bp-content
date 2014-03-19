#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import ConfigParser
import argparse
import codecs
import json
import logging
import math
import os
import pickle
import random
import re
import sys
import time

# Third-party imports
import numpy as np
import pytumblr

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from authors.BluePeninsulaUtility import BluePeninsulaUtility

class TumblrSources:
    """Represents a collection of Tumblr blogs selected by getting
    tagged posts.

    """
    def __init__(self, config_file, source_word_str,
                 consumer_key="7c3XQwWIUJS9hjJ9EPzhx2qlySQ5J2sIRgXRN89Ld03AGtK1KP",
                 secret_key="R8Y1Qj7wODcorDid3A24Ct1bfUg0wGoT9iB4n2GgXwKcTb6csb",
                 number_of_api_attempts=4, seconds_between_api_attempts=1):
        """Constructs a TumblrSources instance given a configuration
        file and source word.

        """
        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.blu_pen_utl = BluePeninsulaUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.blu_pen_utl.process_source_words(source_word_str)

        # Assign input atributes
        self.consumer_key = consumer_key
        self.secret_key = secret_key
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts
        # TODO: Process a configuration file to set the pickle directory
        self.content_dir = os.path.join(self.config.get("tumblr", "content_dir"), self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        # Initialize created attributes
        self.host_names = set()
        self.blog_posts = []

        # Create a client
        self.client = pytumblr.TumblrRestClient(self.consumer_key, self.secret_key)

        # Create a logger
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        if len(root.handlers) == 0:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root.addHandler(console_handler)
            file_handler = logging.FileHandler("TumblrSources.log", mode='w', encoding='utf-8')
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        self.logger = logging.getLogger(u"TumblrSources")

        # Check input arguments
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg)
        if not self.source_type == "#":
            err_msg = u"{0} can only search for posts with tag (#)".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg)

    def get_blog_names_by_posts_with_tag(self, source_type, source_word, limit=20, before=0):
        """Makes multiple attempts to get blog host names by posts with
        tag, sleeping before attempts.

        """
        # Initialize return value
        exc_caught = False
        host_names = set()
        time_stamp = [float('inf')]

        def make_attempt():
            """Makes a single attempt to get blog host names by posts with
            tag, sleeping before the attempt.

            """
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

                # Collect uniaue blog names from posts
                for post in posts:
                    host_names.add(post['blog_name'])
                    if post['timestamp'] < time_stamp[0]:
                        time_stamp[0] = post['timestamp']

                self.logger.info(u"{0}: found {1} blog host names by posts with {2}{3}".format(
                    self.source_log, len(host_names), source_type, source_word))

            except Exception as exc:
                exc_caught = True
                self.logger.warning(u"{0}: couldn't find blog host names by posts with {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        # Make attempts to get blog host names by posts with tag
        iAttempts = 1
        make_attempt()
        while exc_caught and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return {'host_names': host_names, 'time_stamp': time_stamp[0]}

    def get_blog_info_by_hostname(self, hostname):
        """Makes multiple attempts to get blog info by hostname,
        sleeping before attempts.

        """
        # Initialize return value
        exc_caught = False
        info = {}

        def make_attempt():
            """Makes a single attempt to get blog info by hostname,
            sleeping before the attempt.

            """
            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get blog info by hostname
            try:
                info.update(self.client.blog_info(hostname)['blog'])

                self.logger.info(u"{0}: found blog info for {1}".format(
                    self.source_log, hostname))

            except Exception as exc:
                exc_caught = True
                self.logger.warning(u"{0}: couldn't find blog info for {1}: {2}".format(
                    self.source_log, hostname, exc))

        # Make attempts to get blog info by hostname
        iAttempts = 1
        make_attempt()
        while exc_caught and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return info

    def get_blog_posts_by_hostname(self, hostname, limit=20, ptype="", offset=0):
        """Makes multiple attempts to get blog posts by hostname,
        sleeping before attempts.

        """
        # Initialize return value
        exc_caught = False
        b_p = {}

        def make_attempt():
            """Makes a single attempt to get blog posts by hostname,
            sleeping before the attempt.

            """
            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get blog posts by hostname
            try:
                if ptype == "" and offset == 0:
                    b_p.update(self.client.posts(hostname, limit=limit))

                elif not ptype == "" and offset == 0:
                    b_p.update(self.client.posts(hostname, limit=limit, type=ptype))

                elif ptype == "" and offset > 0:
                    b_p.update(self.client.posts(hostname, limit=limit, offset=offset))

                else:
                    b_p.update(self.client.posts(hostname, limit=limit, type=ptype, offset=offset))

                self.logger.info(u"{0}: found blog posts for {1}".format(
                    self.source_log, hostname))

            except Exception as exc:
                exc_caught = True
                self.logger.warning(u"{0}: couldn't find blog posts for {1}: {2}".format(
                    self.source_log, hostname, exc))

        # Make attempts to get blog info by hostname
        iAttempts = 1
        make_attempt()
        while exc_caught and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return b_p

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
        """Dumps TumblrSources attributes pickle.

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
        """Loads TumblrSources attributes pickle.

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

if __name__ == "__main__":
    """Selects a set of Tumblr blogs by getting tagged posts.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Selects a set of Tumblr blogs by getting tagged posts")
    parser.add_argument("-c", "--config-file",
                        default="../authors/BluePeninsula.cfg",
                        help="the configuration file")
    parser.add_argument("-w", "--source-words-file",
                        default="./TumblrSources.json",
                        help="the tag, with leading '#', to search for posts")
    args = parser.parse_args()

    # Load the source words file
    inp = codecs.open(args.source_words_file, encoding='utf-8', mode='r')
    source = json.loads(inp.read())
    inp.close()

    # Consider each source word string
    host_names = []
    blog_posts = []
    for source_word_str in source['words']:

        # Create a TumblrSources instance, and create the content directory, if needed
        ts = TumblrSources(args.config_file, source_word_str)
        if not os.path.exists(ts.content_dir):
            os.makedirs(ts.content_dir)
        ts.logger.info(u"{0}: finding blogs using {1}".format(
            ts.source_log, source_word_str))

        # Create and dump, or load, the TumblrSources pickle
        if not os.path.exists(ts.pickle_file_name):

            # Get blog names by posts with tag
            ns = ts.get_blog_names_by_posts_with_tag(ts.source_type, ts.source_word)
            ts.host_names = ts.host_names.union(ns['host_names'])
            n_host_names = len(ts.host_names)
            additional_names_found = True
            ts.logger.info(u"{0}: found {1} unique blog host names using {2}".format(
                ts.source_log, n_host_names, source_word_str))
            while additional_names_found and n_host_names < 1000:
                ns = ts.get_blog_names_by_posts_with_tag(ts.source_type, ts.source_word, before=ns['time_stamp'])
                ts.host_names = ts.host_names.union(ns['host_names'])
                if len(ts.host_names) > n_host_names:
                    additional_names_found = True
                else:
                    additional_names_found = False
                n_host_names = len(ts.host_names)
                ts.logger.info(u"{0}: found {1} unique blog host names using {2}".format(
                    ts.source_log, n_host_names, source_word_str))
        
            # Get information, total number of posts, and text posts
            # for each blog
            for host_name in ts.host_names:
                b_p = ts.get_blog_posts_by_hostname(host_name)
                ts.blog_posts.append(b_p)

            ts.dump()

        else:
            
            ts.load()

        # Accumulate blog info, and posts
        for b_p in ts.blog_posts:
            if not 'blog' in b_p:
                continue
            h_n = b_p['blog']['name']
            if not h_n in host_names:
                host_names.append(h_n)
                blog_posts.append(b_p)

    # Consider sample posts from each blog
    total_tags = []
    for blog in blog_posts:

        # If there are no posts for the current blog, note that the
        # total number of tag appearances is zero, and continue to the
        # next blog
        n_tags = 0
        if not 'posts' in blog:
            total_tags.append(n_tags)
            continue

        # Consider each post from the current blog
        posts = blog['posts']
        for post in posts:

            # Consider each source word
            for source_word_str in source['words']:

                # Process the source word string to create log and
                # path strings, and assign input argument attributes
                (source_log,
                 source_path,
                 source_header,
                 source_label,
                 source_type,
                 source_word) = ts.blu_pen_utl.process_source_words(source_word_str)

                # Count the appearances of the current source word in
                # the curren post of the current blog
                n_tags += len(re.findall(source_word, "".join(post['tags']), re.I))

        # Note the total number of tag appearances for the current
        # blog
        total_tags.append(n_tags)

    # Find the blogs with the highest number of tag appearances
    # TODO: Remove the hard coded values
    np_total_tags = np.array(total_tags)
    min_total_tags = 40
    index_blog, = np.nonzero(np_total_tags > min_total_tags)
    while np.size(index_blog) < 100 and min_total_tags > 0:
        min_total_tags -= 1
        index_blog, = np.nonzero(np_total_tags > min_total_tags)

    # Select the blogs with the highest number of tag appearances
    blogs_info = []
    posts = []
    likes = []
    for i_blg in index_blog:
        info = blog_posts[i_blg]['blog']
        blogs_info.append(info)
        if 'posts' in info:
            posts.append(info['posts'])
        else:
            posts.append(0)
        if 'likes' in info:
            likes.append(info['likes'])
        else:
            likes.append(0)

    # Compute scores based on number of posts, number of likes,
    # and the likes to posts ratio
    np_n_posts = np.array(posts)
    np_n_likes = np.array(likes)
    np_n_trusting = np_n_likes / np_n_posts

    # Convert the numeric scores to string scores
    np_s_posts = ts.n_to_s(np_n_posts)
    np_s_likes = ts.n_to_s(np_n_likes)
    np_s_trusting = ts.n_to_s(np_n_trusting)

    # Create a dictionary of blogs in order to print a JSON document
    # to a file
    blogs = []
    for i_blg in range(len(blogs_info)):
        blog = {}

        info = blogs_info[i_blg]

        if 'name' in info:
            blog['name'] = info['name']
        else:
            blog['name'] = ""
        if 'title' in info:
            blog['title'] = info['title']
        else:
            blog['title'] = ""
        if 'description' in info:
            blog['description'] = info['description']
        else:
            blog['description'] = ""
        if 'url' in info:
            blog['url'] = info['url']
        else:
            blog['url'] = ""

        blog['posts'] = np_n_posts[i_blg]
        blog['likes'] = np_n_likes[i_blg]
        blog['trusting'] = np_n_trusting[i_blg]
        blog['score'] = np_s_posts[i_blg] + np_s_likes[i_blg] + np_s_trusting[i_blg]

        if blog['score'] == "+++":
            blog['include'] = True
        else:
            blog['include'] = False

        blogs.append(blog)

    # Print the selected blogs JSON document, preserving the encoding
    blogs_file_name = args.source_words_file.replace(".json", ".out")
    out = codecs.open(blogs_file_name, encoding='utf-8', mode='w')
    out.write(json.dumps(blogs, ensure_ascii=False, indent=4, separators=(',', ': ')))
    out.close()
