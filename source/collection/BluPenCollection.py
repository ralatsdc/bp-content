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
from author.BluPenAuthor import BluPenAuthor
from collection.CrisisCollection import CrisisCollection
from utility.QueueUtility import QueueUtility

class BluPenCollection(object):
    """Represents Blue Peninsula collection content.

    """
    def __init__(self, config_file, author_config_file, do_update=False,
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
        self.author_config_file = author_config_file
        self.blu_pen_author = BluPenAuthor(self.author_config_file)

        self.do_update = do_update

        self.collection_requests_dir = self.config.get("collection", "requests_dir")
        self.content_dir = self.config.get("collection", "content_dir")

        self.source_requests_dir = self.config.get("source", "requests_dir")
        self.author_requests_dir = self.config.get("author", "requests_dir")

        self.feed_content_dir = self.config.get("feed", "content_dir")
        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.instagram_content_dir = self.config.get("instagram", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.twitter_content_dir = self.config.get("twitter", "content_dir")

        self.add_console_handler = self.config.getboolean("collection", "add_console_handler")
        self.add_file_handler = self.config.getboolean("collection", "add_file_handler")
        self.log_file_name = self.config.get("collection", "log_file_name")
        log_level = self.config.get("collection", "log_level")
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

        self.pid_file_name = self.config.get("collection", "pid_file_name")

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

    def assemble_crisis_collection(self, country, query):
        """Assemble the crisis collection for the specified country.

        """
        self.logger.info(u"assembling content for {0}".format(country))
        crisis_collection = CrisisCollection(self, country, query)
        crisis_collection.assemble_content(do_update=self.do_update)
        self.logger.info(u"content assembled for {0}".format(country))

if __name__ == "__main__":
    """Assembles content for each collection.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Assembles content for each collection")
    parser.add_argument("-c", "--config-file",
                        default="BluPenCollection.cfg",
                        help="the configuration file")
    parser.add_argument("-a", "--author-config-file",
                        default="../author/BluPenAuthor.cfg",
                        help="the configuration file")
    parser.add_argument("-u", "--do-update",
                        action="store_true",
                        help="update country content")
    args = parser.parse_args()

    # Process the queue, if all previous processes have completed
    # without exception
    try:

        # Assume processing is incomplete
        do_rm_pid_file = False

        # Configure a BluPenCollection
        bpc = BluPenCollection(args.config_file, args.author_config_file, do_update=args.do_update)

        # Write the PID file, or exit if a previous process is
        # running, or exited with an exception
        if not os.path.exists(bpc.pid_file_name):

            # Write the PID file
            pid_file = open(bpc.pid_file_name, 'w')
            pid_file.write(str(os.getpid()))
            pid_file.close()

        else:

            # Exit since a previous process is running, or exited with
            # an exception
            bpc.logger.info("A previous process is running, or exited with an exception")
            sys.exit()

        # Read the input request JSON document from collection/queue
        qu = QueueUtility()
        inp_file_name, inp_req_data = qu.read_queue(bpc.collection_requests_dir)
        out_file_name = os.path.basename(inp_file_name); out_req_data = inp_req_data
        if inp_file_name == "" or inp_req_data == {}:
            bps.logger.info("Nothing to do, scheduling collections updating")
            call(["../scripts/update_collections.sh"])
            sys.exit()

        # Assemble content for the specified collection
        if inp_req_data['collection'] == 'crisis':
            bpc.assemble_crisis_collection(inp_req_data['country'], inp_req_data['query'])

        # Write the input request JSON document to collection/did-pop
        qu.write_queue(bpc.collection_requests_dir, out_file_name, inp_req_data)

        # Write the output request JSON document to source/do-push
        # Not required

        # Assume processing is complete
        do_rm_pid_file = True

    except Exception as exc:

        # Log exceptions
        bpc.logger.error(exc)
    
    finally:

        # Remove the PID file, if processing is complete
        if do_rm_pid_file:
            os.remove(bpc.pid_file_name)
        
