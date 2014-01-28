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
from TwitterAuthors import TwitterAuthors
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
        self.twitter_min_shipped_pages = self.config.getint("twitter", "min_shipped_pages")
        self.twitter_max_shipped_pages = self.config.getint("twitter", "max_shipped_pages")

        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.flickr_min_shipped_pages = self.config.getint("flickr", "min_shipped_pages")
        self.flickr_max_shipped_pages = self.config.getint("flickr", "max_shipped_pages")

        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.tumblr_min_shipped_pages = self.config.getint("tumblr", "min_shipped_pages")
        self.tumblr_max_shipped_pages = self.config.getint("tumblr", "max_shipped_pages")

        self.feed_content_dir = self.config.get("feed", "content_dir")
        self.feed_min_shipped_pages = self.config.getint("feed", "min_shipped_pages")
        self.feed_max_shipped_pages = self.config.getint("feed", "max_shipped_pages")

        self.lulu_publisher = LuluPublisher(self.config.get("lulu", "username"),
                                            self.config.get("lulu", "password"),
                                            self.config.get("lulu", "api_key"))

        self.blu_pen_utility = BluePeninsulaUtility()

        # Get Blue Peninsula Flickr content
        username = "blu_pen"
        flickr_content_dir = os.path.join(self.flickr_content_dir, username)
        self.flickr_author = FlickrAuthor(self, username, flickr_content_dir)
        if not os.path.exists(self.flickr_author.pickle_file_name):
            if not os.path.exists(flickr_content_dir):
                os.makedirs(flickr_content_dir)
            self.flickr_author.set_photosets_as_recent()
            self.flickr_author.set_photos_as_recent()
            self.flickr_author.download_photos()
            self.flickr_author.dump()
        else:
            self.flickr_author.load()

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

    def register_twitter_edition(self, book_title, source_words_string, contents,
                                 do_purge, only_print, notify):
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
        result = self.add_twitter_content(book_title, source_words_string, do_purge=do_purge)

        # Notify the Blue Peninsula user that their edition has been
        # registered
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your edition has been registered...",
                                   result['text_file_name'], result['html_file_name'])

    def add_twitter_content(self, book_title, source_words_string, do_purge=False, task_id=0, add_length=300):
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
        if do_purge and os.path.exists(twitter_authors_full.pickle_file_name):
            os.remove(twitter_authors_full.pickle_file_name)

        # Create a TwitterAuthors instance for full content
        content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_authors_full = TwitterAuthors(self, source_words_string, content_dir)
        if os.path.exists(twitter_authors_full.pickle_file_name):
            twitter_authors_full.load()

        # Get and dump tweets and images, loading the pickle file if
        # it exists
        if not os.path.exists(twitter_authors_full.pickle_file_name):
            # This version of the edition is new
            # Not needed: twitter_authors_full.set_tweets_as_recent()
            twitter_authors_full.set_images_as_recent(self.flickr_author)
            twitter_authors_full.content_set = True
            twitter_authors_full.dump()
        else:
            # This version of the edition is not new
            twitter_authors_full.load()
            if not twitter_authors_full.content_set:
                # Creation of the new version was interrupted
                # Not needed: twitter_authors_full.set_tweets_as_recent()
                twitter_authors_full.set_images_as_recent(self.flickr_author)
                twitter_authors_full.content_set = True
                twitter_authors_full.dump()

        # Create a TwitterAuthors instance for partial content
        content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        twitter_authors_part = TwitterAuthors(self, source_words_string, content_dir,
                                              max_length=np.max(twitter_authors_full.length) + add_length,
                                              number_of_api_attempts=2)

        # Add and dump tweets and images, loading the pickle file if
        # it exists, or assigning full content to partial content if
        # not
        if not os.path.exists(twitter_authors_part.pickle_file_name):
            # This addition of content is new
            # Assign full content to partial content
            twitter_authors_part.page = twitter_authors_full.page
            twitter_authors_part.max_id = twitter_authors_full.max_id
            twitter_authors_part.since_id = twitter_authors_full.since_id
            twitter_authors_part.tweets = twitter_authors_full.tweets
            twitter_authors_part.tweet_id = twitter_authors_full.tweet_id
            twitter_authors_part.created_dt = twitter_authors_full.created_dt
            twitter_authors_part.text_symbol = twitter_authors_full.text_symbol
            twitter_authors_part.clean_text = twitter_authors_full.clean_text
            twitter_authors_part.sidebar_fill_rgb = twitter_authors_full.sidebar_fill_rgb
            twitter_authors_part.link_rgb = twitter_authors_full.link_rgb
            twitter_authors_part.text_rgb = twitter_authors_full.text_rgb
            twitter_authors_part.background_rgb = twitter_authors_full.background_rgb
            twitter_authors_part.length = twitter_authors_full.length
            twitter_authors_part.count = twitter_authors_full.count
            twitter_authors_part.volume = twitter_authors_full.volume
            twitter_authors_part.frequency = twitter_authors_full.frequency
            twitter_authors_part.profile_image_file_name = twitter_authors_full.profile_image_file_name
            twitter_authors_part.background_image_file_name = twitter_authors_full.background_image_file_name
            twitter_authors_part.content_set = twitter_authors_full.content_set

            # Add and dump tweets and images
            if not max(twitter_authors_full.since_id) > 0:
                # Get earlier tweets
                twitter_authors_part.set_tweets_as_recent()
            else:
                # Get later tweets
                twitter_authors_part.set_tweets_as_recent(parameter="since_id")
            twitter_authors_part.set_images_as_recent(self.flickr_author)
            twitter_authors_part.content_set = True
            twitter_authors_part.dump()
        else:
            # This addition of content is not new
            twitter_authors_part.load()
            if not twitter_authors_part.content_set:
                # Creation of the new content was interrupted
                if not max(twitter_authors_full.since_id) > 0:
                    # Get earlier tweets
                    twitter_authors_part.set_tweets_as_recent()
                else:
                    # Get later tweets
                    twitter_authors_part.set_tweets_as_recent(parameter="since_id")
                twitter_authors_part.set_images_as_recent(self.flickr_author)
                twitter_authors_part.content_set = True
                twitter_authors_part.dump()

        # Assign updated partial to original full content, and dump
        twitter_authors_full.page = twitter_authors_part.page
        twitter_authors_full.max_id = twitter_authors_part.max_id
        twitter_authors_full.since_id = twitter_authors_part.since_id
        twitter_authors_full.tweets = twitter_authors_part.tweets
        twitter_authors_full.tweet_id = twitter_authors_part.tweet_id
        twitter_authors_full.created_dt = twitter_authors_part.created_dt
        twitter_authors_full.text_symbol = twitter_authors_part.text_symbol
        twitter_authors_full.clean_text = twitter_authors_part.clean_text
        twitter_authors_full.sidebar_fill_rgb = twitter_authors_part.sidebar_fill_rgb
        twitter_authors_full.link_rgb = twitter_authors_part.link_rgb
        twitter_authors_full.text_rgb = twitter_authors_part.text_rgb
        twitter_authors_full.background_rgb = twitter_authors_part.background_rgb
        twitter_authors_full.length = twitter_authors_part.length
        twitter_authors_full.count = twitter_authors_part.count
        twitter_authors_full.volume = twitter_authors_part.volume
        twitter_authors_full.frequency = twitter_authors_part.frequency
        twitter_authors_full.profile_image_file_name = twitter_authors_part.profile_image_file_name
        twitter_authors_full.background_image_file_name = twitter_authors_part.background_image_file_name
        twitter_authors_full.content_set = twitter_authors_part.content_set
        twitter_authors_full.dump()

        # Convert datetime to needed formats
        title_start_date = twitter_authors_full.created_dt[0].strftime("%B %d, %Y").replace(" 0", " ")
        title_stop_date = twitter_authors_full.created_dt[-1].strftime("%B %d, %Y").replace(" 0", " ")
        
        # Write pending email message
        source_str = output_types[0] + source_header.replace("and ", "and " + output_types[0])
        result = self.write_email_message("twitter", self.twitter_content_dir, content_dir, "pending", source_str, task_id,
                                          book_title=book_title, date_from=title_start_date, date_to=title_stop_date)

        self.logger.info("{0} ({1}) set tweets and images".format(source_log, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_twitter_edition(self, book_title, source_words_string, contents,
                                do_purge, only_print, notify):
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
        twitter_authors_full = TwitterAuthors(self, source_words_string, content_dir)
        if not os.path.exists(twitter_authors_full.pickle_file_name):
            raise IOError("File {0} does not exist.".format(twitter_authors_full.pickle_file_name))
        
        # Trim the common content length for the Twitter edition, if
        # needed
        # NOTE: Not needed

        # Override configuration
        self.use_uuid = False
        
        # Write and process the ConTeXt contents file, rewriting and
        # reprocessing to create the index, and padding with empty
        # pages or shortening the index, if needed
        shipped_pages = self.design_twitter_contents(book_title, source_words_string, contents=contents)

        # Write and process the ConTeXt cover file
        self.design_twitter_cover(book_title, source_words_string,
                                  contents=contents, shipped_pages=shipped_pages)

        # Publish the Twitter edition to Lulu
        if not only_print:
            result = self.publish_twitter_book(book_title, source_words_string, shipped_pages)

        # Notify the Blue Peninsula user that their edition is ready
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your edition is ready!",
                                   result['text_file_name'], result['html_file_name'])

        # Remove the common content pickle for the Twitter edition
        if do_purge:
            os.remove(twitter_authors.pickle_file_name)

    def publish_twitter_authors(self, book_title, source_words_string, contents, do_purge,
                                use_archive, zip_file_name, only_print, notify):
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
        result = self.get_twitter_content(book_title, source_words_string, do_purge=do_purge,
                                          use_archive=use_archive, zip_file_name=zip_file_name)

        # Notify the Blue Peninsula user that their book is being
        # published.
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Write and process the ConTeXt contents file, rewriting and
        # reprocessing to create the index, and padding with empty
        # pages or shortening the index, if needed
        shipped_pages = self.design_twitter_contents(book_title, source_words_string, contents=contents)

        # Write and process the ConTeXt cover file
        self.design_twitter_cover(book_title, source_words_string,
                                  contents=contents, shipped_pages=shipped_pages)

        # Publish the Twitter book to Lulu
        if not only_print:
            result = self.publish_twitter_book(book_title, source_words_string, shipped_pages)

        # Notify the Blue Peninsula user that their book is ready
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your book is ready!",
                                   result['text_file_name'], result['html_file_name'])

    def get_twitter_content(self, book_title, source_words_string, do_purge=True,
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

        # Create a TwitterAuthors instance for full content
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_authors = TwitterAuthors(self, source_words_string, content_dir)

        # Remove pickle file, if needed
        if do_purge and os.path.exists(twitter_authors.pickle_file_name):
            os.remove(twitter_authors.pickle_file_name)

        # Extract zip archive, if needed
        if use_archive:
            TwitterUtility.extract_tweets_from_archive(zip_file_name, content_dir)

        # Get and dump, or first load, tweets and images
        if not os.path.exists(twitter_authors.pickle_file_name):
            if not use_archive:
                twitter_authors.set_tweets_as_recent()
                twitter_authors.set_images_as_recent(self.flickr_author)
            else:
                twitter_authors.set_tweets_from_archive()
            twitter_authors.content_set = True
            twitter_authors.dump()
        else:
            twitter_authors.load()
            if not twitter_authors.content_set:
                if not use_archive:
                    twitter_authors.set_tweets_as_recent()
                    twitter_authors.set_images_as_recent(self.flickr_author)
                else:
                    twitter_authors.set_tweets_from_archive()
                twitter_authors.content_set = True
                twitter_authors.dump()

        # Convert datetime to needed formats
        title_start_date = twitter_authors.created_dt[0].strftime("%B %d, %Y").replace(" 0", " ")
        title_stop_date = twitter_authors.created_dt[-1].strftime("%B %d, %Y").replace(" 0", " ")
        
        # Write pending email message
        source_str = output_types[0] + source_header.replace("and ", "and " + output_types[0])
        result = self.write_email_message("twitter", self.twitter_content_dir, content_dir, "pending", source_str, task_id,
                                          book_title=book_title, date_from=title_start_date, date_to=title_stop_date)

        self.logger.info("{0} ({1}) set tweets and images".format(source_log, self.uuid))

        return {'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def design_twitter_contents(self, book_title, source_words_string, contents="shortwing"):
        """Write and process the ConTeXt contents file, rewriting and
        reprocessing to create the index, and padding with empty pages
        or shortening the index, if needed.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)

        # Load content for the Twitter authors
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_authors = TwitterAuthors(self, source_words_string, content_dir)
        twitter_authors.load()

        # Write contents ConTeXt file
        contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
        twitter_authors.write_shortwing_contents(book_title, contents_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(source_log, self.uuid, contents_inp_file_name))
        
        # Process contents ConTeXt file
        contents_inp_file_name = source_path + "_contents.tex"
        contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
        self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(source_log, self.uuid, contents_inp_file_name))

        # Count pages
        shipped_pages = self.count_pages(contents_out_file_name)
        self.logger.info("{0} ({1}) produced {2} pages".format(source_log, self.uuid, shipped_pages))

        # Process ConTeXt generated *.tuc file to produce register
        # entries
        contents_tuc_file_name = os.path.join(content_dir, source_path + "_contents.tuc")
        result = self.produce_register_entries(contents_tuc_file_name)
        register = result['register']
        start_page = result['start_page']
        self.logger.info("{0} ({1}) processed {2} found start page {3}".format(
                source_log, self.uuid, contents_tuc_file_name, str(start_page)))

        # Rewrite contents ConTeXt file
        contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
        twitter_authors.write_shortwing_contents(book_title, contents_inp_file_name,
                                                 register=register, start_page=start_page)
        self.logger.info("{0} ({1}) rewrote {2}".format(source_log, self.uuid, contents_inp_file_name))

        # Reprocess contents ConTeXt file
        contents_inp_file_name = source_path + "_contents.tex"
        contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
        self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
        self.logger.info("{0} ({1}) reprocessed {2}".format(source_log, self.uuid, contents_inp_file_name))

        # Count pages
        shipped_pages = self.count_pages(contents_out_file_name)
        self.logger.info("{0} ({1}) produced {2} pages".format(source_log, self.uuid, shipped_pages))

        # Pad with empty pages, if required
        if int(shipped_pages) < self.twitter_min_shipped_pages:
            empty_pages = self.twitter_min_shipped_pages - int(shipped_pages)
            
            # Rewrite contents ConTeXt file with empty pages
            contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
            twitter_authors.write_shortwing_contents(book_title, contents_inp_file_name,
                                                     register=register, start_page=start_page,
                                                     empty_pages=empty_pages)
            self.logger.info("{0} ({1}) rewrote {2}".format(source_log, self.uuid, contents_inp_file_name))
            self.logger.info("{0} ({1}) padded with {2} empty pages".format(
                    source_log, self.uuid, empty_pages))

            # Reprocess contents ConTeXt file
            contents_inp_file_name = source_path + "_contents.tex"
            contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
            self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
            self.logger.info("{0} ({1}) reprocessed {2}".format(
                    source_log, self.uuid, contents_inp_file_name))

            # Count pages
            shipped_pages = self.count_pages(contents_out_file_name)
            self.logger.info("{0} ({1}) produced {2} pages".format(source_log, self.uuid, shipped_pages))

        # Shorten index, if required
        if int(shipped_pages) > self.twitter_max_shipped_pages:
            min_frequency = 2

            # Rewrite contents ConTeXt file with shortened index
            contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
            twitter_authors.write_shortwing_contents(book_title, contents_inp_file_name,
                                                     register=register, start_page=start_page,
                                                     min_frequency=min_frequency)
            self.logger.info("{0} ({1}) rewrote {2}".format(source_log, self.uuid, contents_inp_file_name))
            self.logger.info("{0} ({1}) required index entries to occur at least {2} times".format(
                    source_log, self.uuid, min_frequency))

            # Reprocess contents ConTeXt file
            contents_inp_file_name = source_path + "_contents.tex"
            contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
            self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
            self.logger.info("{0} ({1}) reprocessed {2}".format(
                    source_log, self.uuid, contents_inp_file_name))

            # Count pages
            shipped_pages = self.count_pages(contents_out_file_name)
            self.logger.info("{0} ({1}) produced {2} pages".format(source_log, self.uuid, shipped_pages))

        # Create contents preview images
        preview_pages = range(1,13)
        preview_pages.extend(range(int(shipped_pages) - 13, int(shipped_pages)))
        self.create_contents_preview(preview_pages, content_dir, source_path)

        return shipped_pages

    def design_twitter_cover(self, book_title, source_words_string, contents="shortwing", shipped_pages="0"):
        """Write and process the ConTeXt cover file.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)

        # Load content for the Twitter authors
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_authors = TwitterAuthors(self, source_words_string, content_dir)
        twitter_authors.load()

        # Get the cover size
        cover_size = self.lulu_publisher.compute_cover_size(shipped_pages, trim_size='POCKET')

        # Write cover ConTeXt file
        cover_inp_file_name = os.path.join(content_dir, source_path + "_cover.tex")
        twitter_authors.write_shortwing_cover(book_title, cover_size, cover_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(source_log, self.uuid, cover_inp_file_name))

        # Process cover ConTeXt file
        cover_inp_file_name = source_path + "_cover.tex"
        cover_out_file_name = os.path.join(content_dir, source_path + "_cover.out")
        self.process_context_file(content_dir, cover_inp_file_name, cover_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(source_log, self.uuid, cover_inp_file_name))

        # Create cover preview images
        self.create_cover_preview(cover_size, content_dir, source_path)

    def publish_twitter_book(self, book_title, source_words_string, shipped_pages, task_id=0):
        """Publish the Twitter book to Lulu.

        """
        # Process source words
        (source_log,
         source_path,
         source_header,
         source_label,
         output_types,
         output_words,
         output_urls) = self.blu_pen_utility.process_source_words(source_words_string)

        # Load content for the Twitter authors
        if self.use_uuid:
            content_dir = os.path.join(self.twitter_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.twitter_content_dir, source_path)
        twitter_authors = TwitterAuthors(self, source_words_string, content_dir)
        twitter_authors.load()

        # Count tweets
        number_tweets = len(twitter_authors.clean_text)

        # Convert datetime to needed formats
        desc_start_date = twitter_authors.created_dt[0].strftime("%B %d, %Y").replace(" 0", " ")
        desc_stop_date = twitter_authors.created_dt[-1].strftime("%B %d, %Y").replace(" 0", " ")
        requested_date_time = (self.requested_dt.strftime("%B %d, %Y").replace(" 0", " ")
                               + " at " + re.sub("^0", "", self.requested_dt.strftime("%I:%M%p")).lower())
        title_start_date = twitter_authors.created_dt[0].strftime("%B %d, %Y").replace(" 0", " ")
        title_stop_date = twitter_authors.created_dt[-1].strftime("%B %d, %Y").replace(" 0", " ")
        
        # Publish the Twitter book
        first_name = "Blue"
        last_name = "Peninsula"
        description = ("Contains {number_tweets} tweets posted to Twitter ".format(
                number_tweets=number_tweets)
                       + "{source_label} from {start_date} to {stop_date}. ".format(
                source_label=source_label, start_date=desc_start_date, stop_date=desc_stop_date)
                       + "Black and white, paperback, {shipped_pages} pages. ".format(
                shipped_pages=shipped_pages)
                       + "Published on demand by Blue Peninsula on {requested_date_time}.".format(
                requested_date_time=requested_date_time))
        contents_pdf_file_name = os.path.join(content_dir, source_path + "_contents.pdf")
        cover_pdf_file_name = os.path.join(content_dir, source_path + "_cover.pdf")
        result = self.lulu_publisher.publish(
            book_title, first_name, last_name, description,
            cover_pdf_file_name, contents_pdf_file_name, shipped_pages,
            trim_size='POCKET')
        id_at_lulu = result['content_id']
        price_at_lulu = result['total_price']
        url_at_lulu = "http://www.lulu.com/content/" + str(result['content_id'])
        url_at_ep ="http://" + self.host_name + "/twitter/status/" + str(task_id) + "/"

        # Write success email message
        source_str = output_types[0] + source_header.replace("and ", "and " + output_types[0])
        result = self.write_email_message("twitter", self.twitter_content_dir, content_dir, "success", source_str, task_id,
                                          book_title=book_title, date_from=title_start_date, date_to=title_stop_date,
                                          url_at_lulu=url_at_lulu)

        self.logger.info("{0} ({1}) published on Lulu".format(source_log, self.uuid))

        return {'id_at_lulu': id_at_lulu, 'price_at_lulu': price_at_lulu, 'url_at_lulu': url_at_lulu,
                'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_flickr_author(self, username, contents, do_purge, only_print, notify):
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
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Write and process the ConTeXt contents file, and padding
        # with empty pages if needed
        shipped_pages = self.design_flickr_contents(username, contents)
        
        # Write and process the ConTeXt cover file
        self.design_flickr_cover(username,
                                 contents=contents, shipped_pages=shipped_pages)

        # Publish the Flickr book to Lulu
        if not only_print:
            result = self.publish_flickr_book(username, shipped_pages)

        # Notify the Flickr author that their book is ready
        if not only_print and notify:
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

    def design_flickr_contents(self, username, contents="boulat"):
        """Write and process the ConTeXt contents file, and padding
        with empty pages if needed.

        """
        # Load content for the Flickr author
        if self.use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, username, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, username)
        flickr_author = FlickrAuthor(self, username, content_dir)
        flickr_author.load()

        # Write contents ConTeXt file
        contents_inp_file_name = os.path.join(content_dir, username + "_contents.tex")
        if contents == "boulat":
            flickr_author.write_boulat_contents(contents_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(username, self.uuid, contents_inp_file_name))

        # Process contents ConTeXt file
        contents_inp_file_name = username + "_contents.tex"
        contents_out_file_name = os.path.join(content_dir, username + "_contents.out")
        self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(username, self.uuid, contents_inp_file_name))

        # Count pages
        shipped_pages = self.count_pages(contents_out_file_name)
        self.logger.info("{0} ({1}) produced {2} pages".format(username, self.uuid, shipped_pages))

        # Pad with empty pages, if required
        if int(shipped_pages) < self.flickr_min_shipped_pages:
            empty_pages = self.flickr_min_shipped_pages - int(shipped_pages)
            
            # Rewrite contents ConTeXt file with empty pages
            contents_inp_file_name = os.path.join(content_dir, username + "_contents.tex")
            if contents == "boulat":
                flickr_author.write_boulat_contents(contents_inp_file_name, empty_pages)
            self.logger.info("{0} ({1}) rewrote {2}".format(username, self.uuid, contents_inp_file_name))
            self.logger.info("{0} ({1}) padded with {2} empty pages".format(
                    username, self.uuid, empty_pages))

            # Reprocess contents ConTeXt file
            contents_inp_file_name = username + "_contents.tex"
            contents_out_file_name = os.path.join(content_dir, username + "_contents.out")
            self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
            self.logger.info("{0} ({1}) reprocessed {2}".format(username, self.uuid, contents_inp_file_name))

            # Count pages
            shipped_pages = self.count_pages(contents_out_file_name)
            self.logger.info("{0} ({1}) produced {2} pages".format(username, self.uuid, shipped_pages))

        # Create contents preview images
        preview_pages = range(1,13)
        preview_pages.extend(range(int(shipped_pages) - 13, int(shipped_pages)))
        self.create_contents_preview(preview_pages, content_dir, username)

        return shipped_pages

    def design_flickr_cover(self, username, contents="boulat", shipped_pages="0"):
        """Write and process the ConTeXt cover file.

        """
        # Load content for the Flickr author
        if self.use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, username, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, username)
        flickr_author = FlickrAuthor(self, username, content_dir)
        flickr_author.load()

        # Get the cover size
        cover_size = self.lulu_publisher.get_cover_size(shipped_pages, trim_size='LARGE_SQUARE')

        # Write cover ConTeXt file
        cover_inp_file_name = os.path.join(content_dir, username + "_cover.tex")
        if contents == "boulat":
            flickr_author.write_boulat_cover(cover_size, cover_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(username, self.uuid, cover_inp_file_name))

        # Process cover ConTeXt file
        cover_inp_file_name = username + "_cover.tex"
        cover_out_file_name = os.path.join(content_dir, username + "_cover.out")
        self.process_context_file(content_dir, cover_inp_file_name, cover_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(username, self.uuid, cover_inp_file_name))

        # Create cover preview images
        self.create_cover_preview(cover_size, content_dir, username)

    def publish_flickr_book(self, username, shipped_pages, task_id=0):
        """Publish the Flickr book to Lulu.

        """
        # Load content for the Flickr author
        if self.use_uuid:
            content_dir = os.path.join(self.flickr_content_dir, username, self.uuid)
        else:
            content_dir = os.path.join(self.flickr_content_dir, username)
        flickr_author = FlickrAuthor(self, username, content_dir)
        flickr_author.load()

        # Convert datetime to needed formats
        requested_date_time = (self.requested_dt.strftime("%B %d, %Y").replace(" 0", " ")
                               + " at " + re.sub("^0", "", self.requested_dt.strftime("%I:%M%p")).lower())

        # Publish the book
        book_title = "@" + username + ": A Digital Life"
        first_name = "Blue"
        last_name = "Peninsula"
        description = ("Every photo by @" + username + ". "
                       + "Color, paperback, " + shipped_pages + " pages. "
                       + "Limited edition 1/1. Produced on-demand by Blue Peninsula on "
                       + requested_date_time + ".")
        contents_pdf_file_name = os.path.join(content_dir, username + "_contents.pdf")
        cover_pdf_file_name = os.path.join(content_dir, username + "_cover.pdf")
        result = self.lulu_publisher.publish(
            book_title, first_name, last_name, description,
            cover_pdf_file_name, contents_pdf_file_name, shipped_pages,
            trim_size='LARGE_SQUARE', color=True)
        id_at_lulu = result['content_id']
        price_at_lulu = result['total_price']
        url_at_lulu = "http://www.lulu.com/content/" + str(result['content_id'])
        url_at_ep ="http://" + self.host_name + "/flickr/status/" + str(task_id) + "/"

        # Write success email message
        source_str = "@" + username
        result = self.write_email_message("flickr", self.flickr_content_dir, content_dir, "success", source_str, task_id,
                                          url_at_lulu=url_at_lulu)
        
        self.logger.info("{0} ({1}) published on Lulu".format(username, self.uuid))

        return {'id_at_lulu': id_at_lulu, 'price_at_lulu': price_at_lulu, 'url_at_lulu': url_at_lulu,
                'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_tumblr_author(self, book_title, subdomain, contents, do_purge, only_print, notify):
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
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Write and process the ConTeXt contents file, and padding
        # with empty pages if needed
        shipped_pages = self.design_tumblr_contents(book_title, subdomain, contents)
        
        # Write and process the ConTeXt cover file
        self.design_tumblr_cover(book_title, subdomain,
                                 contents=contents, shipped_pages=shipped_pages)

        # Publish the Tumblr book to Lulu
        if not only_print:
            result = self.publish_tumblr_book(book_title, subdomain, shipped_pages)

        # Notify the Tumblr author that their book is ready
        if not only_print and notify:
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

    def design_tumblr_contents(self, book_title, subdomain, contents="shortwing"):
        """Write and process the ConTeXt contents filew, and padding
        with empty pages if needed.

        """
        # Load content for the Tumblr author
        if self.use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self, subdomain, content_dir)
        tumblr_author.load()

        # Write contents ConTeXt file
        contents_inp_file_name = os.path.join(content_dir, subdomain + "_contents.tex")
        if contents == "wheat":
            tumblr_author.write_wheat_contents(book_title, contents_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(subdomain, self.uuid, contents_inp_file_name))

        # Process contents ConTeXt file
        contents_inp_file_name = subdomain + "_contents.tex"
        contents_out_file_name = os.path.join(content_dir, subdomain + "_contents.out")
        self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(subdomain, self.uuid, contents_inp_file_name))

        # Count pages
        shipped_pages = self.count_pages(contents_out_file_name)
        self.logger.info("{0} ({1}) produced {2} pages".format(subdomain, self.uuid, shipped_pages))

        # Pad with empty pages, if required
        if int(shipped_pages) < self.tumblr_min_shipped_pages:
            empty_pages = self.tumblr_min_shipped_pages - int(shipped_pages)
            
            # Rewrite contents ConTeXt file with empty pages
            contents_inp_file_name = os.path.join(content_dir, subdomain + "_contents.tex")
            if contents == "wheat":
                tumblr_author.write_wheat_contents(book_title, contents_inp_file_name, empty_pages)
            self.logger.info("{0} ({1}) rewrote {2}".format(subdomain, self.uuid, contents_inp_file_name))
            self.logger.info("{0} ({1}) padded with {2} empty pages".format(
                    subdomain, self.uuid, empty_pages))

            # Reprocess contents ConTeXt file
            contents_inp_file_name = subdomain + "_contents.tex"
            contents_out_file_name = os.path.join(content_dir, subdomain + "_contents.out")
            self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
            self.logger.info("{0} ({1}) reprocessed {2}".format(subdomain, self.uuid, contents_inp_file_name))

            # Count pages
            shipped_pages = self.count_pages(contents_out_file_name)
            self.logger.info("{0} ({1}) produced {2} pages".format(subdomain, self.uuid, shipped_pages))

        # Create contents preview images
        preview_pages = range(1,13)
        preview_pages.extend(range(int(shipped_pages) - 13, int(shipped_pages)))
        self.create_contents_preview(preview_pages, content_dir, subdomain)

        return shipped_pages

    def design_tumblr_cover(self, book_title, subdomain, contents="shortwing", shipped_pages="0"):
        """Write and process the ConTeXt cover file.

        """
        # Load content for the Tumblr author
        if self.use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self, subdomain, content_dir)
        tumblr_author.load()

        # Get the cover size
        cover_size = self.lulu_publisher.get_cover_size(shipped_pages, trim_size='CROWN_QUARTO')

        # Write cover ConTeXt file
        cover_inp_file_name = os.path.join(content_dir, subdomain + "_cover.tex")
        if contents == "wheat":
            tumblr_author.write_wheat_cover(book_title, cover_size, cover_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(subdomain, self.uuid, cover_inp_file_name))

        # Process cover ConTeXt file
        cover_inp_file_name = subdomain + "_cover.tex"
        cover_out_file_name = os.path.join(content_dir, subdomain + "_cover.out")
        self.process_context_file(content_dir, cover_inp_file_name, cover_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(subdomain, self.uuid, cover_inp_file_name))

        # Create cover preview images
        self.create_cover_preview(cover_size, content_dir, subdomain)

    def publish_tumblr_book(self, book_title, subdomain, shipped_pages, task_id=0):
        """Publish the Tumblr book to Lulu.

        """
        # Load content for the Tumblr author
        if self.use_uuid:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain, self.uuid)
        else:
            content_dir = os.path.join(self.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self, subdomain, content_dir)
        tumblr_author.load()

        # Convert datetime to needed formats
        requested_date_time = (self.requested_dt.strftime("%B %d, %Y").replace(" 0", " ")
                               + " at " + re.sub("^0", "", self.requested_dt.strftime("%I:%M%p")).lower())

        # Publish the book
        # book_title = "@" + subdomain + ": A Digitial Life"
        first_name = "Blue"
        last_name = "Peninsula"
        description = ("Every post by @" + subdomain + ". "
                       + "Color, paperback, " + shipped_pages + " pages. "
                       + "Limited edition 1/1. Produced on-demand by Blue Peninsula on "
                       + requested_date_time + ".")
        contents_pdf_file_name = os.path.join(content_dir, subdomain + "_contents.pdf")
        cover_pdf_file_name = os.path.join(content_dir, subdomain + "_cover.pdf")
        result = self.lulu_publisher.publish(
            book_title, first_name, last_name, description,
            cover_pdf_file_name, contents_pdf_file_name, shipped_pages,
            trim_size='CROWN_QUARTO', color=True);
        id_at_lulu = result['content_id']
        price_at_lulu = result['total_price']
        url_at_lulu = "http://www.lulu.com/content/" + str(result['content_id'])
        url_at_ep ="http://" + self.host_name + "/tumblr/status/" + str(task_id) + "/"

        # Write success email message
        source_str = "@" + subdomain
        result = self.write_email_message("tumblr", self.tumblr_content_dir, content_dir, "success", source_str, task_id,
                                          url_at_lulu=url_at_lulu)

        self.logger.info("{0} ({1}) published on Lulu".format(subdomain, self.uuid))

        return {'id_at_lulu': id_at_lulu, 'price_at_lulu': price_at_lulu, 'url_at_lulu': url_at_lulu,
                'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def publish_feed_author(self, book_title, source_url, contents, do_purge, only_print, notify):
        """Publish a feed author.
        
        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")
        self.logger.info("{0} ({1}) == publish feed author ==".format(source_netloc, self.uuid))

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
        if not only_print and notify:
            self.send_notification(self.to_email_address, "Your book is being published...",
                                   result['text_file_name'], result['html_file_name'])

        # Write and process the ConTeXt contents file, padding with
        # empty pages if needed
        shipped_pages = self.design_feed_contents(book_title, source_url, contents)
        
        # Write and process the ConTeXt cover file
        self.design_feed_cover(book_title, source_url,
                               contents=contents, shipped_pages=shipped_pages)

        # Publish the feed book to Lulu
        if not only_print:
            result = self.publish_feed_book(book_title, source_url, shipped_pages)

        # Notify the feed author that their book is ready
        if not only_print and notify:
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

    def design_feed_contents(self, book_title, source_url, contents="smallwhite"):
        """Write and process the ConTeXt contents filew padding with
        empty pages if needed.

        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")

        # Load content for the feed author
        if self.use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        feed_author = FeedAuthor(self, source_url, content_dir)
        feed_author.load()

        # Write contents ConTeXt file
        contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
        if contents == "smallwhite":
            feed_author.write_smallwhite_contents(book_title, contents_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(source_netloc, self.uuid, contents_inp_file_name))

        # Process contents ConTeXt file
        contents_inp_file_name = source_path + "_contents.tex"
        contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
        self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(source_netloc, self.uuid, contents_inp_file_name))

        # Count pages
        shipped_pages = self.count_pages(contents_out_file_name)
        self.logger.info("{0} ({1}) produced {2} pages".format(source_netloc, self.uuid, shipped_pages))

        # Pad with empty pages, if required
        if int(shipped_pages) < self.feed_min_shipped_pages:
            empty_pages = self.feed_min_shipped_pages - int(shipped_pages)
            
            # Rewrite contents ConTeXt file with empty pages
            contents_inp_file_name = os.path.join(content_dir, source_path + "_contents.tex")
            if contents == "smallwhite":
                feed_author.write_smallwhite_contents(book_title, contents_inp_file_name, empty_pages)
            self.logger.info("{0} ({1}) rewrote {2}".format(source_netloc, self.uuid, contents_inp_file_name))
            self.logger.info("{0} ({1}) padded with {2} empty pages".format(
                    source_netloc, self.uuid, empty_pages))

            # Reprocess contents ConTeXt file
            contents_inp_file_name = source_path + "_contents.tex"
            contents_out_file_name = os.path.join(content_dir, source_path + "_contents.out")
            self.process_context_file(content_dir, contents_inp_file_name, contents_out_file_name)
            self.logger.info("{0} ({1}) reprocessed {2}".format(source_netloc, self.uuid, contents_inp_file_name))

            # Count pages
            shipped_pages = self.count_pages(contents_out_file_name)
            self.logger.info("{0} ({1}) produced {2} pages".format(source_netloc, self.uuid, shipped_pages))

        # Create contents preview images
        preview_pages = range(1,13)
        preview_pages.extend(range(int(shipped_pages) - 13, int(shipped_pages)))
        self.create_contents_preview(preview_pages, content_dir, source_path)

        return shipped_pages

    def design_feed_cover(self, book_title, source_url, contents="shortwing", shipped_pages="0"):
        """Write and process the ConTeXt cover file.

        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")

        # Load content for the Feed author
        if self.use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        feed_author = FeedAuthor(self, source_url, content_dir)
        feed_author.load()

        # Get the cover size
        cover_size = self.lulu_publisher.get_cover_size(shipped_pages, trim_size='POCKET')

        # Write cover ConTeXt file
        cover_inp_file_name = os.path.join(content_dir, source_path + "_cover.tex")
        if contents == "smallwhite":
            feed_author.write_smallwhite_cover(book_title, cover_size, cover_inp_file_name)
        self.logger.info("{0} ({1}) wrote {2}".format(source_netloc, self.uuid, cover_inp_file_name))

        # Process cover ConTeXt file
        cover_inp_file_name = source_path + "_cover.tex"
        cover_out_file_name = os.path.join(content_dir, source_path + "_cover.out")
        self.process_context_file(content_dir, cover_inp_file_name, cover_out_file_name)
        self.logger.info("{0} ({1}) processed {2}".format(source_netloc, self.uuid, cover_inp_file_name))

        # Create cover preview images
        self.create_cover_preview(cover_size, content_dir, source_path)

    def publish_feed_book(self, book_title, source_url, shipped_pages, task_id=0):
        """Publish the feed book to Lulu.

        """
        # Process source URL
        source_netloc = urlparse(source_url).netloc
        source_path = source_netloc.replace(".", "_")

        # Load content for the Feed author
        if self.use_uuid:
            content_dir = os.path.join(self.feed_content_dir, source_path, self.uuid)
        else:
            content_dir = os.path.join(self.feed_content_dir, source_path)
        feed_author = FeedAuthor(self, source_url, content_dir)
        feed_author.load()

        # Convert datetime to needed formats
        created_dt = datetime.now()
        created_date_time = (created_dt.strftime("%B %d, %Y").replace(" 0", " ")
                            + " at " + re.sub("^0", "", created_dt.strftime("%I:%M%p")).lower())

        # Publish the book
        # book_title = "@" + source_url + ": A Digitial Life"
        first_name = "Blue"
        last_name = "Peninsula"
        description = ("Content by " + source_netloc + ". "
                       + "Color, paperback, " + shipped_pages + " pages. "
                       + "Limited edition 1/1. Produced on-demand by Blue Peninsula on "
                       + created_date_time + ".")
        contents_pdf_file_name = os.path.join(content_dir, source_path + "_contents.pdf")
        cover_pdf_file_name = os.path.join(content_dir, source_path + "_cover.pdf")
        result = self.lulu_publisher.publish(
            book_title, first_name, last_name, description,
            cover_pdf_file_name, contents_pdf_file_name, shipped_pages,
            trim_size='POCKET')
        id_at_lulu = result['content_id']
        price_at_lulu = result['total_price']
        url_at_lulu = "http://www.lulu.com/content/" + str(result['content_id'])
        url_at_ep ="http://" + self.host_name + "/feed/status/" + str(task_id) + "/"

        # Write success email message
        source_str = "@" + source_url
        result = self.write_email_message("feed", self.feed_content_dir, content_dir, "success", source_str, task_id,
                                          url_at_lulu=url_at_lulu)

        self.logger.info("{0} ({1}) published on Lulu".format(source_netloc, self.uuid))

        return {'id_at_lulu': id_at_lulu, 'price_at_lulu': price_at_lulu, 'url_at_lulu': url_at_lulu,
                'text_file_name': result['text_file_name'], 'html_file_name': result['html_file_name']}

    def process_context_file(self, content_dir, inp_file_name, out_file_name):
        """Process a ConTeXt file.

        """
        out_file = open(out_file_name, 'w')
        out_file.write(str(datetime.now()))
        out_file.flush()
        os.fsync(out_file.fileno()),
        if self.use_batch_mode:
            subprocess.call(["context", "--batch", inp_file_name],
                            cwd=content_dir, stdout=out_file, stderr=subprocess.STDOUT)
        else:
            subprocess.call(["context", inp_file_name],
                            cwd=content_dir, stdout=out_file, stderr=subprocess.STDOUT)
        out_file.close()

    def count_pages(self, out_file_name):
        """Process ConTeXt generated *.out file to count pages.

        """
        shipped_pages = "0"
        try:
            out_file = open(out_file_name, 'r')
            for line in out_file:
                if line.find("shipped pages") > 0:
                    p = re.compile(", (\d+) shipped pages")
                    shipped_pages = p.search(line).group(1)
                    break
            out_file.close()
        except Exception as exc:
            raise ProcessingError("Could not process .out file: {0}".format(str(exc)))
        if shipped_pages == "0":
            raise ProcessingError("No pages shipped")
        return shipped_pages

    def produce_register_entries(self, tuc_file_name):
        """Process ConTeXt generated *.tuc file to produce register
        entries.

        """
        p_word = re.compile('{ "(.*)",')
        p_page = re.compile('\["realpage"\]=(.*),')
        try:
            tuc_file = open(tuc_file_name, 'r')
            register = {}
            parse_word = False
            parse_page = False
            start_page = sys.maxint
            for line in tuc_file:
                if line.find('["list"]={') != -1 and line.find('["list"]={}') == -1:
                    parse_word = True
                elif parse_word:
                    parse_word = False
                    parse_page = True
                    word = p_word.search(line).group(1)
                elif parse_page and line.find('["realpage"]=') != -1:
                    parse_page = False
                    page = int(p_page.search(line).group(1))
                    if page < start_page:
                        start_page = page
                    if word in register:
                        register[word].add(page)
                    else:
                        register[word] = set([page])
            tuc_file.close()
        except Exception as exc:
            raise ProcessingError("Could not process .tuc file: {0}".format(str(exc)))

        return {'register': register, 'start_page': start_page}

    def create_contents_preview(self, preview_pages, content_dir, source_path):
        """Create contents preview images.

        """
        # Create contents preview PDF
        page_selection = str(preview_pages).replace("[", "").replace(" ","").replace("]", "")
        contents_inp_file_name = source_path + "_contents.pdf"
        contents_res_file_name = source_path + "_contents_preview"
        contents_out_file_name = os.path.join(content_dir, source_path + "_contents_texexec.out")
        contents_out_file = open(contents_out_file_name, 'w')
        contents_out_file.write(str(datetime.now()))
        contents_out_file.flush()
        os.fsync(contents_out_file.fileno()),
        subprocess.call(["texexec", contents_inp_file_name,
                         "--pdfcopy", "--pages=" + page_selection, "--scale=1000",
                         "--result=" + contents_res_file_name],
                        cwd=content_dir, stdout=contents_out_file, stderr=subprocess.STDOUT)
        contents_out_file.close()

        # Create contents preview PNGs
        contents_inp_file_name = source_path + "_contents_preview.pdf"
        contents_res_file_name = source_path + "_contents_preview.png"
        contents_out_file_name = os.path.join(content_dir, source_path + "_contents_convert.out")
        contents_out_file = open(contents_out_file_name, 'w')
        contents_out_file.write(str(datetime.now()))
        contents_out_file.flush()
        os.fsync(contents_out_file.fileno()),
        subprocess.call(["convert", "-density", "144", contents_inp_file_name, "-size", "50%",
                         "+adjoin", contents_res_file_name],
                        cwd=content_dir, stdout=contents_out_file, stderr=subprocess.STDOUT)
        contents_out_file.close()

    def create_cover_preview(self, cover_size, content_dir, source_path):
        """Create cover preview images.

        """
        # Determine cover dimensions in pixels
        cover_inp_file_name = source_path + "_cover.pdf"
        cover_res_file_name = source_path + "_cover.png"
        cover_out_file_name = os.path.join(content_dir, source_path + "_cover_convert.out")
        cover_out_file = open(cover_out_file_name, 'w')
        cover_out_file.write(str(datetime.now()))
        cover_out_file.flush()
        os.fsync(cover_out_file.fileno()),
        subprocess.call(["convert", cover_inp_file_name, cover_res_file_name],
                        cwd=content_dir, stdout=cover_out_file, stderr=subprocess.STDOUT)
        cover_out_file.close()
        cover_png_file_name = os.path.join(content_dir, source_path + "_cover.png")
        img = Image.open(cover_png_file_name)
        cover_paper_width = float(
            cover_size['coverSizeData']['fullCoverDimension']['width']['valueInInches'])
        # TODO: Use these lines to account for trim. Watch units.
        # cover_page_width = cover_paper_width - self.lulu_trim - self.cover_trim + self.cover_trim
        spine_width = float(
            cover_size['coverSizeData']['spineWidth']['valueInInches'])
        spine_paper_width = 100.0 * spine_width / cover_paper_width # Percent
        preview_paper_width = (100.0 - spine_paper_width) / 2.0 # Percent

        # Create front cover preview PNG
        cover_res_file_name = source_path + "_cover_front.png"
        cover_out_file_name = os.path.join(content_dir, source_path + "_cover_front.out")
        cover_out_file = open(cover_out_file_name, 'w')
        cover_out_file.write(str(datetime.now()))
        cover_out_file.flush()
        os.fsync(cover_out_file.fileno()),
        subprocess.call(["convert", cover_inp_file_name, "-crop", "{0}%x100+{1}+0".format(
                    int(preview_paper_width), int((preview_paper_width + spine_paper_width) * img.size[0] / 100.0)),
                         cover_res_file_name],
                        cwd=content_dir, stdout=cover_out_file, stderr=subprocess.STDOUT)
        cover_out_file.close()

        # Create back cover preview PNG
        cover_res_file_name = source_path + "_cover_back.png"
        cover_out_file_name = os.path.join(content_dir, source_path + "_cover_back.out")
        cover_out_file = open(cover_out_file_name, 'w')
        cover_out_file.write(str(datetime.now()))
        cover_out_file.flush()
        os.fsync(cover_out_file.fileno()),
        subprocess.call(["convert", cover_inp_file_name, "-crop", "{0}%x100+0+0".format(
                    int(preview_paper_width)), cover_res_file_name],
                        cwd=content_dir, stdout=cover_out_file, stderr=subprocess.STDOUT)
        cover_out_file.close()

    def write_email_message(self, source, source_dir, content_dir, file_root, source_str, task_id,
                            book_title="", date_from="", date_to="", url_at_lulu=""):
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
        text = text.replace("{book_title}", book_title.encode('ascii', 'ignore'))
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
        if task_id == 0:
            text = text.replace("{url_at_ep}", url_at_lulu)
        else:
            text = text.replace("{url_at_ep}", url_at_ep)
        text_file.write(text)
        text_file.close()

        # Write email message HTML
        html_file_name = os.path.join(source_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='r', encoding='utf-8', errors='ignore')
        html = html_file.read()
        html_file.close()
        html_file_name = os.path.join(content_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='w', encoding='ascii', errors='ignore')
        html = html.replace("{book_title}", book_title.encode('ascii', 'ignore'))
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
        if task_id == 0:
            html = html.replace("{url_at_ep}", url_at_lulu)
        else:
            html = html.replace("{url_at_ep}", url_at_ep)
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
    # TODO: Enforce command line option consistency
    blu_pen = BluePeninsula("BluePeninsula.cfg")
     
    parser = argparse.ArgumentParser(description="Publish a Twitter, Tumblr, or Flickr author.")
     
    parser.add_argument("service", metavar="service",
                        choices=["twitter", "tumblr", "flickr", "feed"],
                        help="the service to publish: twitter, tumblr, flickr, or feed")

    parser.add_argument("contents", metavar="contents",
                        choices=["shortwing", "wheat", "boulat", "smallwhite"],
                        help="the contents to produce: "
                        + "shortwing (if twitter), "
                        + "wheat (if tumblr), "
                        + "boulat (if flickr), "
                        + "or smallwhite (if feed)")

    parser.add_argument("publication", metavar="publication",
                        choices=["book", "edition"],
                        help="the publication to create: book or edition")

    default=u'Skitsnack p\xe5 140 tecken.'
    parser.add_argument("-t", "--title",
                        default="The Digital Life: A Memoir",
                        help="the title of the Twitter, Tumblr, Flickr, or Feed book")

    parser.add_argument("-w", "--source-words-string",
                        default="ladygaga",
                        help="the names, or tags, of the Twitter, Tumblr, Flickr, or Feed author, or content")

    parser.add_argument("-p", "--do-purge", action="store_true",
                        help="create and dump, or load, tweets, posts, or photosets")

    parser.add_argument("-a", "--only-add-content", action="store_true",
                        help="only add content, do not print or publish the edition")

    parser.add_argument("-z", "--zip-file-name",
                        default="",
                        help="a zip file containing archived content of the Twitter author")

    parser.add_argument("-e", "--only-publish-edition", action="store_true",
                        help="only print or publish the edition, do not add content")

    parser.add_argument("-o", "--only-print", action="store_true",
                        help="only print, do not publish the book or edition to Lulu")

    parser.add_argument("-n", "--notify", action="store_true",
                        help="notify by email")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="echo log messages to the console")
 
    # If verbose, echo log messages to the console
    blu_pen.args = parser.parse_args()
    if blu_pen.args.verbose:
        root.addHandler(console_handler)
 
    # Publish Twitter, Flickr, Tumblr, or Feed authors
    if blu_pen.args.service == "twitter":
        if blu_pen.args.publication == "edition":
            if not blu_pen.args.only_publish_edition:
                blu_pen.register_twitter_edition(blu_pen.args.title,
                                                     blu_pen.args.source_words_string,
                                                     blu_pen.args.contents,
                                                     blu_pen.args.do_purge,
                                                     blu_pen.args.only_print,
                                                     blu_pen.args.notify)
            if not blu_pen.args.only_add_content:
                blu_pen.publish_twitter_edition(blu_pen.args.title,
                                                    blu_pen.args.source_words_string,
                                                    blu_pen.args.contents,
                                                    blu_pen.args.do_purge,
                                                    blu_pen.args.only_print,
                                                    blu_pen.args.notify)

        else: # publication = "book"
            if len(blu_pen.args.zip_file_name) == 0:
                use_archive = False
                zip_file_name = ""
                source_words_string = blu_pen.args.source_words_string

            else:
                use_archive = True
                zip_file_name = blu_pen.args.zip_file_name
                source_words_string = "@" + TwitterUtility.get_name_from_archive(zip_file_name)

            blu_pen.publish_twitter_authors(blu_pen.args.title,
                                                source_words_string,
                                                blu_pen.args.contents,
                                                blu_pen.args.do_purge,
                                                use_archive,
                                                zip_file_name,
                                                blu_pen.args.only_print,
                                                blu_pen.args.notify)

    elif blu_pen.args.service == "flickr":
        if blu_pen.args.publication == "edition":
            pass

        else: # publication = "book"
            blu_pen.publish_flickr_author(blu_pen.args.source_words_string,
                                              blu_pen.args.contents,
                                              blu_pen.args.do_purge,
                                              blu_pen.args.only_print,
                                              blu_pen.args.notify)

    elif blu_pen.args.service == "tumblr":
        if blu_pen.args.publication == "edition":
            pass

        else: # publication = "book"
            blu_pen.publish_tumblr_author(blu_pen.args.title,
                                              blu_pen.args.source_words_string,
                                              blu_pen.args.contents,
                                              blu_pen.args.do_purge,
                                              blu_pen.args.only_print,
                                              blu_pen.args.notify)

    elif blu_pen.args.service == "feed":
        if blu_pen.args.publication == "edition":
            pass

        else: # publication = "book"
            blu_pen.publish_feed_author(blu_pen.args.title,
                                            blu_pen.args.source_words_string,
                                            blu_pen.args.contents,
                                            blu_pen.args.do_purge,
                                            blu_pen.args.only_print,
                                            blu_pen.args.notify)
