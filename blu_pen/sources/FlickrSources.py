#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
import ConfigParser
import argparse
import codecs
import json
import logging
import math
import os
import pickle
import random
import sys
import time

# Third-party imports
import numpy as np
import flickrapi

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from authors.BluePeninsulaUtility import BluePeninsulaUtility

class FlickrSources:
    """Represents a collection of Flickr groups selected by searching
    for groups using a query term.

    """
    def __init__(self, config_file, source_word_str,
                 api_key='71ae5bd2b331d44649161f6d3ff7e6b6', api_secret='45f1be4bd59f9155',
                 number_of_api_attempts=4, seconds_between_api_attempts=1):
        """Constructs a FlickrUsers instance given a source word.

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
        if len(self.source_word) > 1:
            err_msg = "{0} only one source word accepted".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg)

        # Assign atributes
        self.api_key = api_key
        self.api_secret = api_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts
        # TODO: Process a configuration file to set the pickle directory
        self.content_dir = os.path.join(self.config.get("flickr", "content_dir"), self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        # TODO: Add attributes
        self.groups = []

        # Create an API
        self.api = flickrapi.FlickrAPI(self.api_key)

        # Create a logger
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        if len(root.handlers) == 0:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root.addHandler(console_handler)
        else:
            for handler in root.handlers:
                if isinstance(handler, logging.StreamHandler):
                    handler.setFormatter(formatter)
                else:
                    root.removeHandler(handler)
        self.logger = logging.getLogger("FlickrSources")

    def get_groups_by_source(self, source_type, source_word, per_page=100, page=0):
        """Makes multiple attempts to get groups by source, sleeping
        before attempts.

        """
        # Initialize return value
        groups = []

        def make_attempt():
            """Makes a single attempt to get groups by source,
            sleeping before the attempt.

            """
            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get groups by source
            try:

                # Use the page parameter, if present
                if page == 0:
                    groups_xml = self.api.groups_search(text=source_word, per_page=per_page)

                else:
                    groups_xml = self.api.groups_search(text=source_word, per_page=per_page, page=page)

                # Parse the resulting XML
                gs = groups_xml.find("groups").findall("group")
                for g in gs:
                    group = {}
                    group['nsid'] = g.get('nsid')
                    group['name'] = g.get('name')
                    group['eighteenplus'] = g.get('eighteenplus')
                    groups.append(group)

                self.logger.info("{0}: found groups for {1}{2}".format(
                    self.source_log, source_type, source_word))

            except Exception as exc:
                self.logger.warning("{0}: couldn't find groups for {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        # Make attempts to get groups by source
        iAttempts = 1
        make_attempt()
        while len(groups) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return groups

    def dump(self, pickle_file_name=None):
        """Dumps FlickrUsers attributes pickle.

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

        p['api_key'] = self.api_key
        p['api_secret'] = self.api_secret
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['groups'] = self.groups

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped {1} groups to {2}".format(
            self.source_log, len(self.groups), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads FlickrUsers attributes pickle.

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

        self.api_key = p['api_key']
        self.api_secret = p['api_secret']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.groups = p['groups']

        self.logger.info("{0} loaded {1} groups from {2}".format(
            self.source_log, len(self.groups), pickle_file_name))

        pickle_file.close()

if __name__ == "__main__":
    """Selects a set of Flickr groups by searching using a query term.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Select a set of Flickr groups by searching using a query term")
    parser.add_argument("-c", "--config-file",
                        default="../authors/BluePeninsula.cfg",
                        help="the configuration file")
    parser.add_argument("-w", "--source-words-str",
                        default="@Japan",
                        help="the query term, with leading '@' for groups, or '#' for photos, search")
    args = parser.parse_args()

    # Create a FlickrUsers instance, and create the content directory,
    # if needed
    fs = FlickrSources(args.config_file, args.source_words_str)
    if not os.path.exists(fs.content_dir):
        os.makedirs(fs.content_dir)

    # Create and dump, or load, the FlickrSources pickle
    if not os.path.exists(fs.pickle_file_name):

        # Select one hundred groups
        fs.groups = fs.get_groups_by_source(fs.source_type[0], fs.source_word[0])
        fs.dump()

    else:
        fs.load()

    """
    # Compute z-scores based on number of statuses, number of
    # followers, and the followers to statuses ratio
    n_statuses = np.array(fs.statuses_count)
    n_followers = np.array(fs.followers_count)
    n_trusting = n_followers / n_statuses

    l_statuses = np.log(n_statuses)
    l_followers = np.log(n_followers)
    l_trusting = np.log(n_trusting)

    z_statuses = (n_statuses - n_statuses.mean()) / n_statuses.std()
    z_followers = (n_followers - n_followers.mean()) / n_followers.std()
    z_trusting = (n_trusting - n_trusting.mean()) / n_trusting.std()

    # Convert the numeric scores to string scores
    s_statuses = fs.z_to_s(z_statuses)
    s_followers = fs.z_to_s(z_followers)
    s_trusting = fs.z_to_s(z_trusting)

    # Create a dictionary of groups in order to print a JSON document
    # to a file
    groups = []
    n_grp = len(fs.groups)
    for i_usr in range(n_grp):
        group = {}
        group['name'] = fs.name[i_grp]
        groups.append(group)

    # Print the selected groups JSON document, preserving the encoding
    groups_file_name = fs.pickle_file_name.replace(".pkl", ".out")
    out = codecs.open(groups_file_name, encoding='utf-8', mode='w')
    out.write(json.dumps(groups, ensure_ascii=False, indent=4, separators=(',', ': ')))
    out.close()
    """
