#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
import codecs
import ConfigParser
import argparse
import datetime
import logging
import os
import sys
import urlparse
import uuid

# Third-party imports

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from FeedAuthor import FeedAuthor
from FlickrGroup import FlickrGroup
from InstagramAuthor import InstagramAuthor
from ProcessingError import ProcessingError
from TumblrAuthor import TumblrAuthor
from TwitterAuthor import TwitterAuthor
from TwitterUtility import TwitterUtility
from utility.QueueUtility import QueueUtility

class BluPenAuthors:
    """Represents Blue Peninsula source authors content.

    """
    def __init__(self, config_file,
                 uuid=str(uuid.uuid4()), requested_dt=datetime.datetime.now()):
        """Constructs a BluPenAuthors instance.

        """
        # Identify this instance
        self.uuid = uuid
        self.requested_dt = requested_dt

        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Assign atributes
        self.authors_requests_dir = self.config.get("authors", "requests_dir")
        self.packages_requests_dir = self.config.get("packages", "requests_dir")
        self.feed_content_dir = self.config.get("feed", "content_dir")
        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.instagram_content_dir = self.config.get("instagram", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.twitter_content_dir = self.config.get("twitter", "content_dir")

        # Create logger, handler, and formatter and set logging level
        # TODO: Standardize and move to a utility
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        for handler in root.handlers:
            root.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)
        file_handler = logging.FileHandler("BluPenAuthor.log", mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        self.logger = logging.getLogger("BluPenAuthor")

    def collect_feed_author_content(self, source_url, use_uuid=False, do_purge=False):
        """Collect content created by a feed author.

        """
        # Create the content directory for the feed author, if needed
        self.logger.info("collecting feed content")
        if use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Remove pickle file, if requested
        feed_author = FeedAuthor(self, source_url, content_dir)
        if do_purge and os.path.exists(feed_author.pickle_file_name):
            os.remove(feed_author.pickle_file_name)

        # Get and dump, or load, content and download images
        if not os.path.exists(feed_author.pickle_file_name):
            feed_author.set_content()
            feed_author.set_image_urls()
            feed_author.download_images()
            feed_author.dump()
        else:
            feed_author.load()
        self.logger.info("feed content collected")

    def collect_flickr_group_content(self, name, nsid, use_uuid=False, do_purge=True):
        """Collect content created by a Flickr group.
        
        """
        # Create the content directory for the Flickr group, if
        # needed
        self.logger.info("collecting flickr content")
        if use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, nsid, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, nsid)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Remove pickle file, if requested
        flickr_group = FlickrGroup(self, u"@" + name, nsid, content_dir)
        if do_purge and os.path.exists(flickr_group.pickle_file_name):
            os.remove(flickr_group.pickle_file_name)

        # Get and dump, or load, photosets, and download photos
        if not os.path.exists(flickr_group.pickle_file_name): 
            flickr_group.set_photos()
            flickr_group.download_photos()
            flickr_group.dump()
        else:
            flickr_group.load()
        self.logger.info("flickr content collected")

    def collect_instagram_author_content(self, source_words_str, use_uuid=False, do_purge=False):
        """Collect content created by a Instagram author.
        
        """
        # Create the content directory for the Instagram author, if needed
        self.logger.info("collecting instagram content")
        if use_uuid:
            content_dir = os.path.join(self.instagram_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.instagram_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Remove pickle file, if requested
        instagram_author = InstagramAuthor(self, source_words_str, content_dir)
        if do_purge and os.path.exists(instagram_author.pickle_file_name):
            os.remove(instagram_author.pickle_file_name)

        # Get and dump, or load, content
        if not os.path.exists(instagram_author.pickle_file_name): 
            instagram_author.set_media()
            instagram_author.download_images()
            instagram_author.dump()
        else:
            instagram_author.load()
        self.logger.info("instagram content collected")

    def collect_tumblr_author_content(self, subdomain, use_uuid=False, do_purge=False):
        """Collect content created by a Tumblr author.
        
        """
        # Create the content directory for the Tumblr author, if
        # needed
        self.logger.info("collecting tumblr content")
        if use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Remove pickle file, get posts, and download photos
        tumblr_author = TumblrAuthor(self, subdomain, content_dir)
        if do_purge and os.path.exists(tumblr_author.pickle_file_name):
            os.remove(tumblr_author.pickle_file_name)

        # Get and dump, or load, posts and download photos
        if not os.path.exists(tumblr_author.pickle_file_name):
            tumblr_author.set_posts()
            tumblr_author.dump()
            tumblr_author.download_photos()
        else:
            tumblr_author.load()
        self.logger.info("tumblr content collected")

    def collect_twitter_author_content(self, source_words_str, zip_file_name="", use_uuid=False, do_purge=False):
        """Collect content created by a Twitter author.
        
        """
        # Create the content directory for the Twitter authors, if
        # needed
        self.logger.info("collecting tumblr content")
        if use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Remove pickle file, if needed
        twitter_author = TwitterAuthor(self, source_words_str, content_dir)
        if do_purge and os.path.exists(twitter_author.pickle_file_name):
            os.remove(twitter_author.pickle_file_name)

        # Extract zip archive, if needed
        use_archive = False
        if zip_file_name != "":
            use_archive = True
            TwitterUtility.extract_tweets_from_archive(zip_file_name, content_dir)

        # Get and dump, or first load, tweets and images
        if not os.path.exists(twitter_author.pickle_file_name):
            if not use_archive:
                twitter_author.set_tweets()
            else:
                twitter_author.set_tweets_from_archive()
            twitter_author.content_set = True
            twitter_author.dump()
        else:
            twitter_author.load()
        self.logger.info("tumblr content collected")

if __name__ == "__main__":
    """Collects content for each source of a collection.
        
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Collects content for each source of a collection")
    parser.add_argument("-c", "--config-file",
                        default="BluPenAuthor.cfg",
                        help="the configuration file")
    args = parser.parse_args()

    # Read the input request JSON document from authors/queue
    qu = QueueUtility()
    bpa = BluPenAuthors(args.config_file)
    inp_file_name, inp_req_data = qu.read_queue(bpa.authors_requests_dir)
    out_file_name = os.path.basename(inp_file_name); out_req_data = {}
    if inp_file_name == "" or inp_req_data == {}:
        bpa.logger.info("Nothing to do, exiting")
        sys.exit()

    # Get author content from the specified service
    if inp_req_data['service'] == 'flickr':
        out_req_data['service'] = 'flickr'
        groups = []
        for group in inp_req_data['groups']:
            if not group['include']:
                continue
            groups.append(group)
            bpa.collect_flickr_group_content(group['name'], group['nsid'])
        out_req_data['groups'] = groups

    elif inp_req_data['service'] == 'tumblr':
        pass

    elif inp_req_data['service'] == 'twitter':
        pass

    # Write the input request JSON document to authors/did-pop
    qu.write_queue(bpa.authors_requests_dir, out_file_name, inp_req_data)

    # Write the output request JSON document to packages/do-push
    qu.write_queue(bpa.packages_requests_dir, out_file_name, out_req_data, status="todo", queue="do-push")
