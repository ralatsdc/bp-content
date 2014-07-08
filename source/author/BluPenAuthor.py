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
import urlparse
from uuid import uuid4

# Third-party imports

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from author.FeedAuthor import FeedAuthor
from author.FlickrGroup import FlickrGroup
from author.InstagramAuthor import InstagramAuthor
from author.TumblrAuthor import TumblrAuthor
from author.TwitterAuthor import TwitterAuthor
from author.TwitterUtility import TwitterUtility
from utility.QueueUtility import QueueUtility

class BluPenAuthor(object):
    """Represents Blue Peninsula source author content.

    """
    def __init__(self, config_file,
                 uuid=uuid4(), requested_dt=datetime.datetime.now()):
        """Constructs a BluPenAuthor instance.

        """
        # Identify this instance
        self.uuid = uuid
        self.requested_dt = requested_dt

        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Assign atributes
        self.author_requests_dir = self.config.get("author", "requests_dir")

        self.do_purge = self.config.getboolean("author", "do_purge")

        self.add_console_handler = self.config.getboolean("author", "add_console_handler")
        self.add_file_handler = self.config.getboolean("author", "add_file_handler")
        self.log_file_name = self.config.get("author", "log_file_name")
        log_level = self.config.get("author", "log_level")
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

        self.collection_requests_dir = self.config.get("collection", "requests_dir")

        self.feed_content_dir = self.config.get("feed", "content_dir")
        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.instagram_content_dir = self.config.get("instagram", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.twitter_content_dir = self.config.get("twitter", "content_dir")

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
        self.logger = logging.getLogger("BluPenAuthor")

    def collect_feed_author_content(self, source_url):
        """Collect content created by a feed author.

        """
        self.logger.info(u"collecting feed content")
        feed_author = FeedAuthor(self, source_url, self.feed_content_dir)
        if not os.path.exists(feed_author.pickle_file_name) or self.do_purge:
            feed_author.set_content(do_purge=self.do_purge)
            feed_author.set_image_urls()
            feed_author.download_images()
            feed_author.dump()
        else:
            feed_author.load()
        self.logger.info(u"feed content collected")

    def collect_flickr_group_content(self, source_word_str, group_id):
        """Collect content created by a Flickr group.

        """
        self.logger.info(u"collecting flickr content")
        flickr_group = FlickrGroup(self, source_word_str, group_id, self.flickr_content_dir)
        if not os.path.exists(flickr_group.pickle_file_name) or self.do_purge:
            flickr_group.set_photos(do_purge=self.do_purge)
            flickr_group.download_photos()
            flickr_group.dump()
        else:
            flickr_group.load()
        self.logger.info(u"flickr content collected")

    def collect_instagram_author_content(self, source_word_str):
        """Collect content created by a Instagram author.

        """
        self.logger.info(u"collecting instagram content")
        instagram_author = InstagramAuthor(self, source_word_str, self.instagram_content_dir)
        if not os.path.exists(instagram_author.pickle_file_name) or self.do_purge:
            instagram_author.set_media(do_purge=self.do_purge)
            instagram_author.download_images()
            instagram_author.dump()
        else:
            instagram_author.load()
        self.logger.info(u"instagram content collected")

    def collect_tumblr_author_content(self, subdomain):
        """Collect content created by a Tumblr author.

        """
        self.logger.info(u"collecting tumblr content")
        tumblr_author = TumblrAuthor(self, subdomain, self.tumblr_content_dir)
        if not os.path.exists(tumblr_author.pickle_file_name) or self.do_purge:
            tumblr_author.set_posts(do_purge=self.do_purge)
            tumblr_author.dump()
            tumblr_author.download_photos()
        else:
            tumblr_author.load()
        self.logger.info(u"tumblr content collected")

    def collect_twitter_author_content(self, source_words_str, zip_file_name=""):
        """Collect content created by a Twitter author.

        """
        self.logger.info(u"collecting twitter content")
        twitter_author = TwitterAuthor(self, source_words_str, self.twitter_content_dir)
        if not os.path.exists(twitter_author.pickle_file_name) or self.do_purge:
            if zip_file_name == "":
                try:
                    twitter_author.set_tweets(do_purge=self.do_purge)
                except Exception as exc:
                    self.logger.warning(u"Failed setting tweets: {0}".format(exc))
            else:
                TwitterUtility.extract_tweets_from_archive(zip_file_name, twitter_author.content_dir)
                twitter_author.set_tweets_from_archive(do_purge=self.do_purge)
            twitter_author.dump()
        else:
            twitter_author.load()
        self.logger.info(u"twitter content collected")

if __name__ == "__main__":
    """Collects content for each source of a collection.

    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Collects content for each source of a collection")
    parser.add_argument("-c", "--config-file",
                        default="BluPenAuthor.cfg",
                        help="the configuration file")
    parser.add_argument("-p", "--do-purge",
                        action="store_true",
                        help="purge existing author")
    args = parser.parse_args()

    # Read the input request JSON document from author/queue
    qu = QueueUtility()
    bpa = BluPenAuthor(args.config_file)
    bpa.do_purge = args.do_purge
    inp_file_name, inp_req_data = qu.read_queue(bpa.author_requests_dir)
    out_file_name = os.path.basename(inp_file_name); out_req_data = {}
    if inp_file_name == "" or inp_req_data == {}:
        bpa.logger.info(u"Nothing to do, exiting")
        sys.exit()

    # Get author content from the specified service
    if inp_req_data['service'] == 'feed':
        bpa.logger.info(u"Getting feed content")
        out_req_data['service'] = 'feed'
        authors = []
        for author in inp_req_data['authors']:
            if not author['include']:
                continue
            authors.append(author)
            source_url = author['url']
            bpa.collect_feed_author_content(source_url)
        out_req_data['authors'] = authors

    elif inp_req_data['service'] == 'flickr':
        bpa.logger.info(u"Getting flickr content")
        out_req_data['service'] = 'flickr'
        groups = []
        for group in inp_req_data['groups']:
            if not group['include']:
                continue
            groups.append(group)
            source_word_str = u'@' + group['name']
            group_id = group["nsid"]
            bpa.collect_flickr_group_content(source_word_str, group_id)
        out_req_data['groups'] = groups

    elif inp_req_data['service'] == 'tumblr':
        bpa.logger.info(u"Getting tumblr content")
        out_req_data['service'] = 'tumblr'
        authors = []
        for author in inp_req_data['authors']:
            if not author['include']:
                continue
            authors.append(author)
            subdomain = urlparse.urlparse(author['url']).netloc
            bpa.collect_tumblr_author_content(subdomain)
        out_req_data['authors'] = authors

    elif inp_req_data['service'] == 'twitter':
        bpa.logger.info(u"Getting twitter content")
        out_req_data['service'] = 'twitter'
        authors = []
        for author in inp_req_data['authors']:
            if not author['include']:
                continue
            authors.append(author)
            source_words_str = u'@' + author['screen_name']
            bpa.collect_twitter_author_content(source_words_str)
        out_req_data['authors'] = authors

    # Write the input request JSON document to author/did-pop
    qu.write_queue(bpa.author_requests_dir, out_file_name, inp_req_data)

    # Write the output request JSON document to collection/do-push
    qu.write_queue(bpa.collection_requests_dir, out_file_name, out_req_data, status="todo", queue="do-push")
