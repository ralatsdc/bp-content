#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
import codecs
import ConfigParser
import argparse
from datetime import datetime, date, timedelta
from logging import handlers
import logging
import os
import re
import shutil
import subprocess
import sys
from urlparse import urlparse
import uuid

# Third-party imports
from PIL import Image
import numpy as np

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility
from FeedAuthor import FeedAuthor
from FlickrAuthor import FlickrAuthor
from ProcessingError import ProcessingError
from TumblrAuthor import TumblrAuthor
from TwitterAuthor import TwitterAuthor
from TwitterUtility import TwitterUtility

class BluePeninsula:
    """Represents Blue Peninsula content.

    """
    def __init__(self, config_file, uuid=str(uuid.uuid4()), requested_dt=datetime.now()):
        """Constructs a BluePeninsula.

        """
        # Identify this instance
        self.uuid = uuid
        self.requested_dt = requested_dt

        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        # Assign atributes
        self.host_name = self.config.get("blu-pen", "host_name")
        self.content_dir = self.config.get("blu-pen", "content_dir")

        self.to_email_address = self.config.get("blu-pen", "to_email_address")
        self.fr_email_address = self.config.get("blu-pen", "fr_email_address")
        self.fr_email_password = self.config.get("blu-pen", "fr_email_password")

        self.to_sms_number = self.config.get("blu-pen", "to_sms_number")
        self.fr_sms_username = self.config.get("blu-pen", "fr_sms_username")
        self.fr_sms_password = self.config.get("blu-pen", "fr_sms_password")
        self.fr_sms_number = self.config.get("blu-pen", "fr_sms_number")

        self.add_console_handler = self.config.getboolean("blu-pen", "add_console_handler")
        self.add_file_handler = self.config.getboolean("blu-pen", "add_file_handler")
        self.log_file_name = self.config.get("blu-pen", "log_file_name")
        log_level = self.config.get("blu-pen", "log_level")
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
        self.use_batch_mode = self.config.getboolean("blu-pen", "use_batch_mode")
        self.use_uuid = self.config.getboolean("blu-pen", "use_uuid")

        self.twitter_content_dir = self.config.get("twitter", "content_dir")
        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.feed_content_dir = self.config.get("feed", "content_dir")

        self.blu_pen_utility = BluePeninsulaUtility()

        # Create logger, handler, and formatter and set logging level
        root = logging.getLogger()
        if self.add_console_handler or self.add_file_handler:
            console_handler = logging.StreamHandler()
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file_name, maxBytes=10000000, backupCount=5)
            formatter = logging.Formatter(
                "%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            if self.add_console_handler:
                root.addHandler(console_handler)
            if self.add_file_handler:
                root.addHandler(file_handler)
        root.setLevel(self.log_level)
        self.logger = logging.getLogger(__name__)

    def collect_feed_author_content(self, source_url, do_purge, notify):
        """Collect content created by a feed author.
        
        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")
        self.logger.info("{0} ({1}) == collect feed author content ==".format(source_netloc, self.uuid))

        # Create the content directory for the feed author, if needed
        if self.use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Get content for the feed author, pickling as needed
        result = self.get_feed_content(source_url, do_purge=do_purge)

        # Notify the Feed author that their book is being published
        if notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Notify the feed author that their book is ready
        if notify:
            self.send_notification(self.to_email_address, "Your book is ready!",
                                   result['text_file_name'], result['html_file_name'])

    def get_feed_content(self, source_url, do_purge=True, task_id=0):
        """Get content for the feed author, pickling as needed.

        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")

        # Get content for the feed author
        if self.use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        feed_author = FeedAuthor(self, source_url, content_dir)

        # Remove pickle file, get content, and download images
        if do_purge and os.path.exists(feed_author.pickle_file_name):
            os.remove(feed_author.pickle_file_name)

        # Get and dump, or load, content and download images
        if not os.path.exists(feed_author.pickle_file_name):
            feed_author.set_content_as_recent()
            feed_author.download_images()
            feed_author.dump()
        else:
            feed_author.load()

        # Write pending email message
        source_str = "@" + source_url
        result = self.write_email_message("feed", self.feed_content_dir, content_dir, "pending", source_str, task_id)

        self.logger.info("{0} ({1}) set posts".format(source_netloc, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_flickr_author(self, username, do_purge, notify):
        """Publish a Flickr author.
        
        """
        self.logger.info("{0} ({1}) == publish flickr author ==".format(username, self.uuid))

        # Create the content directory for the Flickr author, if
        # needed
        if self.use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, username, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, username)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Get content for the Flickr Author, pickling as needed
        result = self.get_flickr_content(username, do_purge=do_purge)

        # Notify the Flickr author that their book is being published
        if notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Notify the Flickr author that their book is ready
        if notify:
            self.send_notification(self.to_email_address, "Your book is ready!",
                                   result['text_file_name'], result['html_file_name'])

    def get_flickr_content(self, username, do_purge=True, task_id=0):
        """Get content for the Flickr author, pickling as needed.

        """
        # Get photosets for the Flickr author
        if self.use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, username, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, username)
        flickr_author = FlickrAuthor(self, username, content_dir)

        # Remove pickle file, get photosets, and download photos
        if do_purge and os.path.exists(flickr_author.pickle_file_name):
            os.remove(flickr_author.pickle_file_name)

        # Get and dump, or load, photosets, and download photos
        if not os.path.exists(flickr_author.pickle_file_name): 
            flickr_author.set_photosets_as_recent()
            flickr_author.set_photos_as_recent()
            flickr_author.download_photos()
            flickr_author.dump()
        else:
            flickr_author.load()

        # Write pending email message
        source_str = "@" + username
        result = self.write_email_message("flickr", self.flickr_content_dir, content_dir, "pending", source_str, task_id)

        self.logger.info("{0} ({1}) set photosets".format(username, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_tumblr_author(self, subdomain, do_purge, notify):
        """Publish a Tumblr author.
        
        """
        self.logger.info("{0} ({1}) == publish tumblr author ==".format(subdomain, self.uuid))

        # Create the content directory for the Tumblr author, if
        # needed
        if self.use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Get content for the Tumblr Author, pickling as needed
        result = self.get_tumblr_content(subdomain, do_purge=do_purge)

        # Notify the Tumblr author that their book is being published
        if notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Notify the Tumblr author that their book is ready
        if notify:
            self.send_notification(self.to_email_address, "Your book is ready!",
                                   result['text_file_name'], result['html_file_name'])

    def get_tumblr_content(self, subdomain, do_purge=True, task_id=0):
        """Get content for the Tumblr author, pickling as needed.

        """
        # Get posts for the Tumblr author
        if self.use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self, subdomain, content_dir)

        # Remove pickle file, get posts, and download photos
        if do_purge and os.path.exists(tumblr_author.pickle_file_name):
            os.remove(tumblr_author.pickle_file_name)

        # Get and dump, or load, posts and download photos
        if not os.path.exists(tumblr_author.pickle_file_name):
            tumblr_author.set_posts_as_recent()
            tumblr_author.download_photos()
            tumblr_author.dump()
        else:
            tumblr_author.load()

        # Write pending email message
        source_str = "@" + subdomain
        result = self.write_email_message("tumblr", self.tumblr_content_dir, content_dir, "pending", source_str, task_id)

        self.logger.info("{0} ({1}) set posts".format(subdomain, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_twitter_author(self, source_words_string, do_purge, use_archive, zip_file_name, notify):
        """Publish Twitter authors.
        
        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)
        self.logger.info("{0} ({1}) == publish twitter authors ==".format(source_log, self.uuid))

        # Create the content directory for the Twitter authors, if
        # needed
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)
        
        # Get content for the Twitter authors, pickling as needed
        result = self.get_twitter_content(source_words_string, do_purge=do_purge,
                                          use_archive=use_archive, zip_file_name=zip_file_name)

        # Notify the Blue Peninsula user that their book is being
        # published.
        if notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Notify the Blue Peninsula user that their book is ready
        if notify:
            self.send_notification(self.to_email_address, "Your book is ready!",
                                   result['text_file_name'], result['html_file_name'])

    def get_twitter_content(self, source_words_string, do_purge=True,
                            use_archive=False, zip_file_name="", task_id=0):
        """Get content for the Twitter authors, pickling as needed.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)

        # Create a TwitterAuthor instance for full content
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_author = TwitterAuthor(self, source_words_string, content_dir)

        # Remove pickle file, if needed
        if do_purge and os.path.exists(twitter_author.pickle_file_name):
            os.remove(twitter_author.pickle_file_name)

        # Extract zip archive, if needed
        if use_archive:
            TwitterUtility.extract_tweets_from_archive(zip_file_name, content_dir)

        # Get and dump, or first load, tweets and images
        if not os.path.exists(twitter_author.pickle_file_name):
            if not use_archive:
                twitter_author.set_tweets_as_recent()
                twitter_author.set_images_as_recent(self.flickr_author)
            else:
                twitter_author.set_tweets_from_archive()
            twitter_author.content_set = True
            twitter_author.dump()
        else:
            twitter_author.load()
            if not twitter_author.content_set:
                if not use_archive:
                    twitter_author.set_tweets_as_recent()
                    twitter_author.set_images_as_recent(self.flickr_author)
                else:
                    twitter_author.set_tweets_from_archive()
                twitter_author.content_set = True
                twitter_author.dump()

        # Write pending email message
        source_str = output_types[0] + source_header.replace("and ", "and " + output_types[0])
        result = self.write_email_message("twitter", self.twitter_content_dir, content_dir, "pending", source_str, task_id)

        self.logger.info("{0} ({1}) set tweets and images".format(source_log, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_twitter_edition(self, source_words_string, do_purge, notify):
        """Publish a Twitter edition.
        
        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)
        self.logger.info("{0} ({1}) == publish twitter edition ==".format(source_log, self.uuid))

        # Check that the common content directory for the Twitter
        # edition exists
        content_dir = os.path.join(self.twitter_content_dir, source_path)
        if not os.path.exists(content_dir):
            raise IOError("Directory {0} does not exist.".format(content_dir))
        
        # Check that the common content pickle for the Twitter edition
        # exists
        twitter_author_full = TwitterAuthor(self, source_words_string, content_dir)
        if not os.path.exists(twitter_author_full.pickle_file_name):
            raise IOError("File {0} does not exist.".format(twitter_author_full.pickle_file_name))
        
        # Trim the common content length for the Twitter edition, if
        # needed
        # NOTE: Not needed

        # Override configuration
        self.use_uuid = False
        
        # Notify the Blue Peninsula user that their edition is ready
        if notify:
            self.send_notification(self.to_email_address, "Your edition is ready!",
                                   result['text_file_name'], result['html_file_name'])

        # Remove the common content pickle for the Twitter edition
        if do_purge:
            os.remove(twitter_author.pickle_file_name)

    def register_twitter_edition(self, source_words_string, do_purge, notify):
        """Register a Twitter edition.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)
        self.logger.info("{0} ({1}) == register twitter edition ==".format(source_log, self.uuid))

        # Create a common content directory for the Twitter edition,
        # if needed
        content_dir = os.path.join(self.twitter_content_dir, source_path)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Create a unique content directory for the Twitter edition,
        # if needed
        content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Add content to the Twitter edition, pickling as needed
        result = self.add_twitter_content(source_words_string, do_purge=do_purge)

        # Notify the Blue Peninsula user that their edition has been
        # registered
        if notify:
            self.send_notification(self.to_email_address, "Your edition has been registered...",
                                   result['text_file_name'], result['html_file_name'])

    def add_twitter_content(self, source_words_string, do_purge=False, task_id=0, add_length=300):
        """Add content to the Twitter edition, pickling as needed.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(twitter_author_full.pickle_file_name):
            os.remove(twitter_author_full.pickle_file_name)

        # Create a TwitterAuthor instance for full content
        content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_author_full = TwitterAuthor(self, source_words_string, content_dir)
        if os.path.exists(twitter_author_full.pickle_file_name):
            twitter_author_full.load()

        # Get and dump tweets and images, loading the pickle file if
        # it exists
        if not os.path.exists(twitter_author_full.pickle_file_name):
            # This version of the edition is new
            # Not needed: twitter_author_full.set_tweets_as_recent()
            twitter_author_full.set_images_as_recent(self.flickr_author)
            twitter_author_full.content_set = True
            twitter_author_full.dump()
        else:
            # This version of the edition is not new
            twitter_author_full.load()
            if not twitter_author_full.content_set:
                # Creation of the new version was interrupted
                # Not needed: twitter_author_full.set_tweets_as_recent()
                twitter_author_full.set_images_as_recent(self.flickr_author)
                twitter_author_full.content_set = True
                twitter_author_full.dump()

        # Create a TwitterAuthor instance for partial content
        content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        twitter_author_part = TwitterAuthor(self, source_words_string, content_dir,
                                            max_length=np.max(twitter_author_full.length) + add_length,
                                            number_of_api_attempts=2)

        # Add and dump tweets and images, loading the pickle file if
        # it exists, or assigning full content to partial content if
        # not
        if not os.path.exists(twitter_author_part.pickle_file_name):
            # This addition of content is new
            # Assign full content to partial content
            twitter_author_part.page = twitter_author_full.page
            twitter_author_part.max_id = twitter_author_full.max_id
            twitter_author_part.since_id = twitter_author_full.since_id
            twitter_author_part.tweets = twitter_author_full.tweets
            twitter_author_part.tweet_id = twitter_author_full.tweet_id
            twitter_author_part.created_dt = twitter_author_full.created_dt
            twitter_author_part.text_symbol = twitter_author_full.text_symbol
            twitter_author_part.clean_text = twitter_author_full.clean_text
            twitter_author_part.sidebar_fill_rgb = twitter_author_full.sidebar_fill_rgb
            twitter_author_part.link_rgb = twitter_author_full.link_rgb
            twitter_author_part.text_rgb = twitter_author_full.text_rgb
            twitter_author_part.background_rgb = twitter_author_full.background_rgb
            twitter_author_part.length = twitter_author_full.length
            twitter_author_part.count = twitter_author_full.count
            twitter_author_part.volume = twitter_author_full.volume
            twitter_author_part.frequency = twitter_author_full.frequency
            twitter_author_part.profile_image_file_name = twitter_author_full.profile_image_file_name
            twitter_author_part.background_image_file_name = twitter_author_full.background_image_file_name
            twitter_author_part.content_set = twitter_author_full.content_set

            # Add and dump tweets and images
            if not max(twitter_author_full.since_id) > 0:
                # Get earlier tweets
                twitter_author_part.set_tweets_as_recent()
            else:
                # Get later tweets
                twitter_author_part.set_tweets_as_recent(parameter="since_id")
            twitter_author_part.set_images_as_recent(self.flickr_author)
            twitter_author_part.content_set = True
            twitter_author_part.dump()
        else:
            # This addition of content is not new
            twitter_author_part.load()
            if not twitter_author_part.content_set:
                # Creation of the new content was interrupted
                if not max(twitter_author_full.since_id) > 0:
                    # Get earlier tweets
                    twitter_author_part.set_tweets_as_recent()
                else:
                    # Get later tweets
                    twitter_author_part.set_tweets_as_recent(parameter="since_id")
                twitter_author_part.set_images_as_recent(self.flickr_author)
                twitter_author_part.content_set = True
                twitter_author_part.dump()

        # Assign updated partial to original full content, and dump
        twitter_author_full.page = twitter_author_part.page
        twitter_author_full.max_id = twitter_author_part.max_id
        twitter_author_full.since_id = twitter_author_part.since_id
        twitter_author_full.tweets = twitter_author_part.tweets
        twitter_author_full.tweet_id = twitter_author_part.tweet_id
        twitter_author_full.created_dt = twitter_author_part.created_dt
        twitter_author_full.text_symbol = twitter_author_part.text_symbol
        twitter_author_full.clean_text = twitter_author_part.clean_text
        twitter_author_full.sidebar_fill_rgb = twitter_author_part.sidebar_fill_rgb
        twitter_author_full.link_rgb = twitter_author_part.link_rgb
        twitter_author_full.text_rgb = twitter_author_part.text_rgb
        twitter_author_full.background_rgb = twitter_author_part.background_rgb
        twitter_author_full.length = twitter_author_part.length
        twitter_author_full.count = twitter_author_part.count
        twitter_author_full.volume = twitter_author_part.volume
        twitter_author_full.frequency = twitter_author_part.frequency
        twitter_author_full.profile_image_file_name = twitter_author_part.profile_image_file_name
        twitter_author_full.background_image_file_name = twitter_author_part.background_image_file_name
        twitter_author_full.content_set = twitter_author_part.content_set
        twitter_author_full.dump()

        # Write pending email message
        source_str = output_types[0] + source_header.replace("and ", "and " + output_types[0])
        result = self.write_email_message("twitter", self.twitter_content_dir, content_dir, "pending", source_str, task_id)

        self.logger.info("{0} ({1}) set tweets and images".format(source_log, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def write_email_message(self, source, source_dir, content_dir, file_root, source_str, task_id):
        """Read, update and write email message text and HTML
        templates in oder to create files needed to send a multipart
        email message.

        """
        # Write email message text
        text_file_name = os.path.join(source_dir, file_root + ".text")
        text_file = codecs.open(text_file_name, mode='r', encoding='utf-8', errors='ignore')
        text = text_file.read()
        text_file.close()
        text_file_name = os.path.join(content_dir, file_root + ".text")
        text_file = codecs.open(text_file_name, mode='w', encoding='ascii', errors='ignore')
        if source == "feed":
            text = text.replace("{source_url}", source_str)
            url_at_ep = "http://" + self.host_name + "/feed/status/" + str(task_id) + "/"
        elif source == "flickr":
            text = text.replace("{username}", source_str)
            url_at_ep = "http://" + self.host_name + "/flickr/status/" + str(task_id) + "/"
        elif source == "tumblr":
            text = text.replace("{subdomain}", source_str)
            url_at_ep = "http://" + self.host_name + "/tumblr/status/" + str(task_id) + "/"
        elif source == "twitter":
            text = text.replace("{screen_name}", source_str)
            text = text.replace("{date_from}", date_from)
            text = text.replace("{date_to}", date_to)
            url_at_ep = "http://" + self.host_name + "/twitter/status/" + str(task_id) + "/"
        else:
            raise Exception("Unknown source.")
        text_file.write(text)
        text_file.close()

        # Write email message HTML
        html_file_name = os.path.join(source_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='r', encoding='utf-8', errors='ignore')
        html = html_file.read()
        html_file.close()
        html_file_name = os.path.join(content_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='w', encoding='ascii', errors='ignore')
        if source == "feed":
            html = html.replace("{source_url}", source_str)
        elif source == "flickr":
            html = html.replace("{username}", source_str)
        elif source == "tumblr":
            html = html.replace("{subdomain}", source_str)
        elif source == "twitter":
            html = html.replace("{screen_name}", source_str)
            html = html.replace("{date_from}", date_from)
            html = html.replace("{date_to}", date_to)
        html_file.write(html)
        html_file.close()

        return {'text_file_name': text_file_name, 'html_file_name': html_file_name}

    def send_notification(self, to_email_address, subject, text_file_name, html_file_name):
        """Send a text and HTML email message to the recipient from
        Blue Peninsula.

        """
        try:
            self.blu_pen_utility.send_mail_html(to_email_address,
                                                    self.fr_email_address,
                                                    self.fr_email_password,
                                                    subject,
                                                    text_file_name,
                                                    html_file_name)
            self.logger.info("notification sent to {0}".format(to_email_address))
        except Exception as exc:
            self.logger.error("notification could not be sent to {0}: {1}".format(
                    to_email_address, exc))

if __name__ == "__main__":
    # Parse command line arguments
    blu_pen = BluePeninsula("BluePeninsula.cfg")
     
    parser = argparse.ArgumentParser(description="Collect Feed, Flickr, Instagram, Tumblr, or Twitter content")
     
    parser.add_argument("service", metavar="service",
                        choices=["feed", "flickr", "instagram", "tumblr", "twitter"],
                        help="the service from which to collect content: feed, flickr, instagram, tumblr, or twitter")

    parser.add_argument("publication", metavar="publication",
                        choices=["book", "edition"],
                        help="collect co: book or edition")

    parser.add_argument("-w", "--source-words-string",
                        default="ladygaga",
                        help="the names, or tags, of the Feed, Flckr, Tumblr, or Twitter author, or content")

    parser.add_argument("-p", "--do-purge", action="store_true",
                        help="create and dump, or load, posts, photosets, or tweets")

    parser.add_argument("-a", "--only-add-content", action="store_true",
                        help="only add content, do not print or publish the edition")

    parser.add_argument("-z", "--zip-file-name",
                        default="",
                        help="a zip file containing archived content of the Twitter author")

    parser.add_argument("-e", "--only-publish-edition", action="store_true",
                        help="only print or publish the edition, do not add content")

    parser.add_argument("-n", "--notify", action="store_true",
                        help="notify by email")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="echo log messages to the console")
 
    # If verbose, echo log messages to the console
    blu_pen.args = parser.parse_args()
    if blu_pen.args.verbose:
        root.addHandler(console_handler)
 
    # Publish Feed, Flickr, Tumblr, or Twitter authors
    if blu_pen.args.service == "feed":
        if blu_pen.args.publication == "edition":
            pass
    
        else: # publication = "book"
            blu_pen.collect_feed_author_content(blu_pen.args.source_words_string,
                                                blu_pen.args.do_purge,
                                                blu_pen.args.notify)

    elif blu_pen.args.service == "flickr":
        if blu_pen.args.publication == "edition":
            pass

        else: # publication = "book"
            blu_pen.publish_flickr_author(blu_pen.args.source_words_string,
                                          blu_pen.args.do_purge,
                                          blu_pen.args.notify)


    elif blu_pen.args.service == "tumblr":
        if blu_pen.args.publication == "edition":
            pass

        else: # publication = "book"
            blu_pen.publish_tumblr_author(blu_pen.args.source_words_string,
                                          blu_pen.args.do_purge,
                                          blu_pen.args.notify)

    elif blu_pen.args.service == "twitter":
        if blu_pen.args.publication == "edition":
            if not blu_pen.args.only_publish_edition:
                blu_pen.register_twitter_edition(blu_pen.args.source_words_string,
                                                 blu_pen.args.do_purge,
                                                 blu_pen.args.notify)

            if not blu_pen.args.only_add_content:
                blu_pen.publish_twitter_edition(blu_pen.args.source_words_string,
                                                blu_pen.args.do_purge,
                                                blu_pen.args.notify)

        else: # publication = "book"
            blu_pen.publish_twitter_author(source_words_string,
                                            blu_pen.args.do_purge,
                                            blu_pen.args.notify)
