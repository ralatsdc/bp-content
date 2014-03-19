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
        """Constructs a FlickrSources instance given a configuration
        file and source word.

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
        self.api_key = api_key
        self.api_secret = api_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts
        # TODO: Process a configuration file to set the pickle directory
        self.content_dir = os.path.join(self.config.get("flickr", "content_dir"), self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")

        # Initialize created attributes
        self.groups = []
        self.nsid = []
        self.name = []
        self.eighteenplus = []
        self.members = []
        self.pool_count = []
        self.topic_count = []
        self.description = []

        # Create an API
        self.api = flickrapi.FlickrAPI(self.api_key)

        # Create a logger
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        for handler in root.handlers:
            root.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)
        file_handler = logging.FileHandler("TumblrSources.log", mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        self.logger = logging.getLogger(u"FlickrSources")

        # Check input arguments
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg)
        if not self.source_type == "@":
            err_msg = u"{0} can only search by group (@)".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg)

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
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
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
                grps = groups_xml.find("groups").findall("group")
                for grp in grps:
                    group = {}
                    group['nsid'] = grp.get('nsid')
                    group['name'] = grp.get('name')
                    group['eighteenplus'] = grp.get('eighteenplus')
                    group['members'] = grp.get('members')
                    group['pool_count'] = grp.get('pool_count')
                    group['topic_count'] = grp.get('topic_count')
                    groups.append(group)

                self.logger.info(u"{0}: found groups for {1}{2}".format(
                    self.source_log, source_type, source_word))

            except Exception as exc:
                self.logger.warning(u"{0}: couldn't find groups for {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        # Make attempts to get groups by source
        iAttempts = 1
        make_attempt()
        while len(groups) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return groups

    def get_group_info_by_id(self, nsid):
        """Makes multiple attempts to get group info by NSID, sleeping
        before attempts.

        """
        # Initialize return value
        info = {}

        def make_attempt():
            """Makes a single attempt to get group info by NSID,
            sleeping before the attempt.

            """
            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get group info by NSID
            try:
                group_info_xml = self.api.groups_getInfo(group_id=nsid)

                # Parse the resulting XML
                info['name'] = group_info_xml.find("group").find("name").text
                info['description'] = group_info_xml.find("group").find("description").text
                info['members'] = int(group_info_xml.find("group").find("members").text)
                info['pool_count'] = int(group_info_xml.find("group").find("pool_count").text)
                info['topic_count'] = int(group_info_xml.find("group").find("topic_count").text)
                self.logger.info(u"{0}: found group info for {1}".format(
                    self.source_log, nsid))

            except Exception as exc:
                self.logger.warning(u"{0}: couldn't find group info for {1}: {2}".format(
                    self.source_log, nsid, exc))

        # Make attempts to get group info by NSID
        iAttempts = 1
        make_attempt()
        while len(info) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1
            make_attempt()

        return info

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
        """Dumps FlickrSources attributes pickle.

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

        p['nsid'] = self.nsid
        p['name'] = self.name
        p['eighteenplus'] = self.eighteenplus
        p['members'] = self.members
        p['pool_count'] = self.pool_count
        p['topic_count'] = self.topic_count
        p['description'] = self.description

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} groups to {2}".format(
            self.source_log, len(self.groups), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads FlickrSources attributes pickle.

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

        self.nsid = p['nsid']
        self.name = p['name']
        self.eighteenplus = p['eighteenplus']
        self.members = p['members']
        self.pool_count = p['pool_count']
        self.topic_count = p['topic_count']
        self.description = p['description']

        self.logger.info(u"{0} loaded {1} groups from {2}".format(
            self.source_log, len(self.groups), pickle_file_name))

        pickle_file.close()

if __name__ == "__main__":
    """Selects a collection of Flickr groups by searching for groups
    using a query term.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Selects a collection of Flickr groups by searching for groups using a query term")
    parser.add_argument("-c", "--config-file",
                        default="../authors/BluePeninsula.cfg",
                        help="the configuration file")
    parser.add_argument("-w", "--source-words-file",
                        default="./FlickrSources.json",
                        help="the tag, with leading '@', to search for groups")
    args = parser.parse_args()

    # Load the source words file
    inp = codecs.open(args.source_words_file, encoding='utf-8', mode='r')
    source = json.loads(inp.read())
    inp.close()

    # Consider each source word string
    nsid = []
    name = []
    eighteenplus = []
    members = []
    pool_count = []
    topic_count = []
    description = []
    for source_word_str in source['words']:

        # Create a FlickrSources instance, and create the content
        # directory, if needed
        fs = FlickrSources(args.config_file, source_word_str)
        if not os.path.exists(fs.content_dir):
            os.makedirs(fs.content_dir)
        fs.logger.info(u"{0}: finding groups using {1}".format(
            fs.source_log, source_word_str))

        # Create and dump, or load, the FlickrSources pickle
        if not os.path.exists(fs.pickle_file_name):

            # Get one hundred groups
            fs.groups = fs.get_groups_by_source(fs.source_type, fs.source_word)

            # Get information for each group
            for grp in fs.groups:
                info = fs.get_group_info_by_id(grp['nsid'])
                grp.update(info)
            
            # Assign arrays of values for selecting groups
            for grp in fs.groups:
                fs.nsid.append(grp['nsid'])
                fs.name.append(grp['name'])
                fs.eighteenplus.append(grp['eighteenplus'])
                fs.members.append(grp['members'])
                fs.pool_count.append(grp['pool_count'])
                fs.topic_count.append(grp['topic_count'])
                fs.description.append(grp['description'])

            fs.dump()

        else:

            fs.load()

        # Accumulate arrays of values for selecting groups
        nsid.extend(fs.nsid)
        name.extend(fs.name)
        eighteenplus.extend(fs.eighteenplus)
        members.extend(fs.members)
        pool_count.extend(fs.pool_count)
        topic_count.extend(fs.topic_count)
        description.extend(fs.description)

    # Compute z-scores based on number of photos, number of members,
    # and the members to photos ratio
    n_photos = np.array(pool_count)
    n_members = np.array(members)
    n_trusting = n_members / n_photos

    # Convert the numeric scores to string scores
    s_photos = fs.n_to_s(n_photos)
    s_members = fs.n_to_s(n_members)
    s_trusting = fs.n_to_s(n_trusting)

    # Create a dictionary of groups in order to print a JSON document
    # to a file
    groups = []
    n_grp = len(nsid)
    for i_grp in range(n_grp):
        group = {}
        group['nsid'] = nsid[i_grp]
        group['name'] = name[i_grp]
        group['eighteenplus'] = eighteenplus[i_grp]
        group['members'] = members[i_grp]
        group['pool_count'] = pool_count[i_grp]
        group['topic_count'] = topic_count[i_grp]
        group['photos'] = n_photos[i_grp]
        group['members'] = n_members[i_grp]
        group['trusting'] = n_trusting[i_grp]
        group['score'] = s_photos[i_grp] + s_members[i_grp] + s_trusting[i_grp]
        if group['score'] == "+++":
            group['include'] = True
        else:
            group['include'] = False
        groups.append(group)

    # Print the selected groups JSON document, preserving the encoding
    groups_file_name = args.source_words_file.replace(".json", ".out")
    out = codecs.open(groups_file_name, encoding='utf-8', mode='w')
    out.write(json.dumps(groups, ensure_ascii=False, indent=4, separators=(',', ': ')))
    out.close()
