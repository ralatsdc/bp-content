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
import flickrapi

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utility.AuthorUtility import AuthorUtility

class FlickrSource(object):
    """Represents a collection of Flickr groups selected by searching
    for groups using a query term.

    """
    def __init__(self, source_word_str, content_dir,
                 api_key='8ffebf639a0d2fd4c13b5fb71cb5ab1b', api_secret='1d23d55f573d5c58',
                 number_of_api_attempts=8, seconds_between_api_attempts=0.1):
        # api_key='71ae5bd2b331d44649161f6d3ff7e6b6', api_secret='45f1be4bd59f9155',
        """Constructs a FlickrSource instance given a configuration
        file and source word.

        """
        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.author_utility = AuthorUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.author_utility.process_source_words(source_word_str)

        # Assign input atributes
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")
        self.api_key = api_key
        self.api_secret = api_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

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
        self.logger = logging.getLogger(u"FlickrSource")

        # Check input arguments
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))
        if not self.source_type == u'@':
            err_msg = u"{0} can only search by group (@)".format(
                self.source_path)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))

    def set_source(self, do_purge=False):
        """Create and dump, or load, the FlickrSource pickle.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the FlickrSource pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0}: finding source using {1}".format(
                self.source_log, self.source_type + self.source_word))

            # Get the default number (100) of groups
            self.groups = self.get_groups_by_source(self.source_type, self.source_word)

            # Get information for each group
            for grp in self.groups:
                info = self.get_group_info_by_id(grp['nsid'])
                grp.update(info)

            # Assign arrays of values for selecting groups
            for group in self.groups:
                self.nsid.append(group['nsid'])
                self.name.append(group['name'])
                self.eighteenplus.append(group['eighteenplus'])
                self.members.append(group['members'])
                self.pool_count.append(group['pool_count'])
                self.topic_count.append(group['topic_count'])
                self.description.append(group['description'])

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_groups_by_source(self, source_type, source_word, per_page=100, page=0):
        """Makes multiple attempts to get groups by source, sleeping
        before attempts.

        """
        groups = None

        # Make multiple attempts to get groups by source
        iAttempts = 0
        while groups is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

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
                groups = []
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
                groups = None
                self.logger.warning(u"{0}: couldn't find groups for {1}{2}: {3}".format(
                    self.source_log, source_type, source_word, exc))

        return groups

    def get_group_info_by_id(self, nsid):
        """Makes multiple attempts to get group info by NSID, sleeping
        before attempts.

        """
        info = None

        # Make multiple attempts to get group info by NSID
        iAttempts = 0
        while info is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before the attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0}: sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make an attempt to get group info by NSID
            try:
                group_info_xml = self.api.groups_getInfo(group_id=nsid)

                # Parse the resulting XML
                info = {}
                info['name'] = group_info_xml.find("group").find("name").text
                info['description'] = group_info_xml.find("group").find("description").text
                info['members'] = int(group_info_xml.find("group").find("members").text)
                info['pool_count'] = int(group_info_xml.find("group").find("pool_count").text)
                info['topic_count'] = int(group_info_xml.find("group").find("topic_count").text)
                self.logger.info(u"{0}: found group info for {1}".format(
                    self.source_log, nsid))

            except Exception as exc:
                info = None
                self.logger.warning(u"{0}: couldn't find group info for {1}: {2}".format(
                    self.source_log, nsid, exc))

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
        """Dumps FlickrSource attributes pickle.

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
        """Loads FlickrSource attributes pickle.

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
