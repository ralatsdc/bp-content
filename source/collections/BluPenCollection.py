#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
import ConfigParser
import argparse
import datetime
import logging
import logging.handlers
import os
import sys
from uuid import uuid4

# Third-party imports

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from collections.CrisisCountryCollection import CrisisCountryCollection
from utility.QueueUtility import QueueUtility

class BluPenCollection:
    """Represents Blue Peninsula collection content.

    """
    def __init__(self, config_file,
                 uuid=uuid4(), requested_dt=datetime.datetime.now()):
        """Constructs a BluPenCollection instance.

        """
        # Identify this instance
                self.uuid = uuid
        self.requested_dt = requested_dt

        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Assign attributes
        self.requests_dir = self.config.get("collections", "requests_dir")
        self.content_dir = self.config.get("collections", "content_dir")

        self.authors_requests_dir = self.config.get("authors", "requests_dir")

        self.feed_content_dir = self.config.get("feed", "content_dir")
        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.instagram_content_dir = self.config.get("instagram", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.twitter_content_dir = self.config.get("twitter", "content_dir")

        self.add_console_handler = self.config.getboolean("collections", "add_console_handler")
        self.add_file_handler = self.config.getboolean("collections", "add_file_handler")
        self.log_file_name = self.config.get("collections", "log_file_name")

        log_level = self.config.get("collections", "log_level")
        if log_level == 'DEBUG':
            self.log_level = logging.DEBUG
        elif log_level == 'INFO':
            self.log_level = logging.INFO
        elif log_level == 'WARNING':
            self.log_level = logging.WARNING
        elif log_level == 'ERROR':
            self.log_level = logging.ERROR
        elif log_level == 'CRITICAL':
            self.log_level = logging.CRITICAL

        # Create a logger
        root = logging.getLogger()
        root.setLevel(self.log_level)
        formatter = logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        for handler in root.handlers:
            root.removeHandler(handler)
        if self.add_console_handler:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root.addHandler(console_handler)
        if self.add_file_handler:
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file_name, maxBytes=1000000, backupCount=5, encoding='utf-8')
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        self.logger = logging.getLogger("BluPenCollection")

    def assemble_crisis_country_collection(self, collection_country):
        """Assemble the collection for the specified crisis country.

        """
        self.logger.info(u"assembling content for {0}".format(collection_country))
        crisis_country_collection = CrisisCountryCollection(self, collection_country)
        crisis_country_collection.assemble_content()
        self.logger.info(u"content assembled for {0}".format(collection_country))

if __name__ == "__main__":
    """Assembles content for each collection.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Assembles content for each collection")
    parser.add_argument("-c", "--config-file",
                        default="BluPenCollection.cfg",
                        help="the configuration file")
    parser.add_argument("-p", "--do-purge",
                        action="store_true",
                        help="purge existing content")
    args = parser.parse_args()

    # Read the input request JSON document from collections/queue
    qu = QueueUtility()
    bpc = BluPenCollection(args.config_file)
    inp_file_name, inp_req_data = qu.read_queue(bpc.requests_dir)
    out_file_name = os.path.basename(inp_file_name); out_req_data = inp_req_data
    if inp_file_name == "" or inp_req_data == {}:
        bpc.logger.info(u"Nothing to do, exiting")
        sys.exit()

    # Assemble content for the specified collection
    if inp_req_data['collection'] == 'crisis-country':
        bpc.assemble_crisis_country_collection(inp_req_data['country'])

    # Write the input request JSON document to authors/did-pop
    qu.write_queue(bpc.requests_dir, out_file_name, inp_req_data)

    # Write the output request JSON document to packages/do-push
    qu.write_queue(bpc.requests_dir, out_file_name, out_req_data, status="todo", queue="do-push")
