# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime
import hashlib
import imghdr
import logging
import math
import os
import pickle
import shutil
from time import sleep
from urlparse import urlparse

# Third-party imports
from lxml.html import soupparser
import pytumblr

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class TumblrAuthor:
    """Represents an author on Tumblr by their creative
    output. Authors are selected by their subdomain.

    """
    def __init__(self, blu_pen, subdomain, content_dir, requested_dt=datetime.now(),
                 consumer_key="7c3XQwWIUJS9hjJ9EPzhx2qlySQ5J2sIRgXRN89Ld03AGtK1KP",
                 secret_key="R8Y1Qj7wODcorDid3A24Ct1bfUg0wGoT9iB4n2GgXwKcTb6csb",
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a TumblrAuthor given a subdomain.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utility = BluePeninsulaUtility()

        self.subdomain = subdomain
        self.content_dir = content_dir
        self.requested_dt = requested_dt
        self.consumer_key = consumer_key
        self.secret_key = secret_key
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        self.pickle_file_name = os.path.join(self.content_dir, self.subdomain + ".pkl")
        
        self.info = None
        self.posts = []
        
        self.client = client = pytumblr.TumblrRestClient(self.consumer_key, self.secret_key)
        self.logger = logging.getLogger(__name__)
        
    def set_posts_as_recent(self, limit=20):
        """Gets recent posts by this author from Tumblr.

        """
        # Get blog information, including the total number of posts
        self.info = self.get_info_by_subdomain(self.subdomain)
        if self.info is None:
            return
        
        # Get sets of posts, containing the maximum number of posts,
        # until all posts are collected
        for offset in range(len(self.posts), self.info['posts'], limit):
            self.posts.extend(self.get_posts_by_subdomain(self.subdomain, limit=limit, offset=offset))
        self.logger.info("{0} collected {1} total posts for {2}".format(
            self.subdomain, len(self.posts), self.subdomain))

        # Put posts in chronological order
        def convert_string_datetime(post):
            posted_dt = datetime.fromtimestamp(post["timestamp"])
            return posted_dt
        self.posts.sort(key=convert_string_datetime)

        # Convert regular posts containing images to photo posts.
        self.convert_text_posts()

    def get_info_by_subdomain(self, subdomain):
        """Makes multiple attempts to get info by subdomain, sleeping
        before attempts.

        """
        info = None

        # Make multiple attempts
        exc = None
        iAttempts = 0
        while info is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep longer before each attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0} sleeping for {1} seconds".format(
                self.subdomain, seconds_between_api_attempts))
            sleep(seconds_between_api_attempts)

            # Attempt to get info by subdomain
            try:
                info = self.client.blog_info(subdomain)['blog']
                self.logger.info("{0} collected info for {1}".format(
                    self.subdomain, subdomain))
            except Exception as exc:
                info = None
                self.logger.warning("{0} couldn't get info for {1}: {2}".format(
                    self.subdomain, subdomain, exc))

        return info

    def get_posts_by_subdomain(self, subdomain, limit=20, offset=0):
        """Makes multiple attempts to get posts by subdomain, sleeping
        before attempts. The maximum number of posts is requested.

        """
        posts = []

        # Make multiple attempts
        exc = None
        iAttempts = 0
        while len(posts) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep longer before each attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0} sleeping for {1} seconds".format(
                self.subdomain, seconds_between_api_attempts))
            sleep(seconds_between_api_attempts)

            # Attempt to get post by subdomain
            try:
                posts = self.client.posts(subdomain, limit=limit, offset=offset)['posts']
                self.logger.info("{0} collected {1} posts for {2}".format(
                    self.subdomain, len(posts), subdomain))
            except Exception as exc:
                posts = []
                self.logger.warning("{0} couldn't get posts for {1}: {2}".format(
                    self.subdomain, subdomain, exc))

        return posts

    def download_photos(self):
        """Download all photos by this author from Tumblr.

        """
        # Consider each photo post
        n_posts = len(self.posts)
        for i_post in range(n_posts):
            if self.posts[i_post]['type'] != "photo":
                continue

            # Consider each photo
            n_photos = len(self.posts[i_post]['photos'])
            for i_photo in range(n_photos):

                # Assign the photo URL by index
                self.posts[i_post]['photos'][i_photo]['alt_sizes_idx'] = 0
                iAS = -1
                for alt_size in self.posts[i_post]['photos'][i_photo]['alt_sizes']:
                    iAS += 1
                    if alt_size['url'].find("_500") != -1:
                        self.posts[i_post]['photos'][i_photo]['alt_sizes_idx'] = iAS
                        break

                # Download the photo, if the photo URL is not empty
                iAS = self.posts[i_post]['photos'][i_photo]['alt_sizes_idx']
                if iAS > -1:
                    photo_url = self.posts[i_post]['photos'][i_photo]['alt_sizes'][iAS]['url']

                    # Create the photo file name
                    head, tail = os.path.split(urlparse(photo_url).path)
                    root, ext = os.path.splitext(tail)
                    if (len(ext) == 0
                        or not ext.lower() in ['.rgb', '.gif', '.pbm', '.pgm', '.ppm', '.tiff', '.tif',
                                               '.rast', '.xbm', '.jpeg', '.jpg', '.bmp', '.png']):
                        ext = ".tmp"
                    if self.posts[i_post]['converted']:
                        tail = hashlib.sha224(photo_url).hexdigest() + ext
                    photo_file_name = os.path.join(self.content_dir, tail)

                    # Download the photo
                    if not os.path.exists(photo_file_name):
                        try:
                            self.blu_pen_utility.download_file(photo_url, photo_file_name)
                            self.logger.info("{0} downloaded photo to file {1}".format(
                                self.subdomain, photo_file_name))

                        except Exception as exc:
                            self.logger.warning("{0} could not download photo at url {1}".format(
                                self.subdomain, photo_url))

                            # Remove the photo file
                            if os.path.exists(photo_file_name):
                                os.remove(photo_file_name)
                                self.logger.info("{0} removed photo file {1}".format(
                                    self.subdomain, photo_file_name))
                            photo_file_name = ""

                    else:
                        self.logger.info("{0} photo already downloaded to file {1}".format(
                            self.subdomain, photo_file_name))

                    # Recreate the photo file name, if the extension
                    # was not defined previously
                    if ext == ".tmp" and not photo_file_name == "":
                        try:
                            ext = "." + imghdr.what(photo_file_name)
                            shutil.move(photo_file_name, photo_file_name.replace(".tmp", ext))
                            photo_file_name = photo_file_name.replace(".tmp", ext)
                            self.logger.info("{0} moved photo file to {1}".format(
                                self.subdomain, photo_file_name))

                        except Exception as exc:
                            self.logger.info("{0} could not move photo file {1}".format(
                                self.subdomain, photo_file_name))

                            # Remove the photo file
                            if os.path.exists(photo_file_name):
                                os.remove(photo_file_name)
                                self.logger.info("{0} removed photo file {1}".format(
                                    self.subdomain, photo_file_name))
                            photo_file_name = ""

                    # Assign the photo file name
                    self.posts[i_post]['photos'][i_photo]['photo-file-name'] = photo_file_name

    def dump(self, pickle_file_name=None):
        """Dump TumblrAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['subdomain'] = self.subdomain
        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['consumer_key'] = self.consumer_key
        p['secret_key'] = self.secret_key
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['posts'] = self.posts
        
        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped {1} posts to {2}".format(
            self.subdomain, len(self.posts), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load TumblrAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.subdomain = p['subdomain']
        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.consumer_key = p['consumer_key']
        self.secret_key = p['secret_key']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.posts = p['posts']

        self.logger.info("{0} loaded {1} posts from {2}".format(
            self.subdomain, len(self.posts), pickle_file_name))

        pickle_file.close()

    def convert_text_posts(self):
        """Convert a text post containing images to a photo post.

        """
        # Consider each post
        n_posts = len(self.posts)
        for i_post in range(n_posts):
            self.posts[i_post]['converted'] = False

            # Convert regular posts only
            if self.posts[i_post]['type'] == "text":

                # Parse the HTML content and consider each element
                root = soupparser.fromstring(self.posts[i_post]['body'])
                for element in root.iter():

                    # Process image elements only
                    if element.tag == 'img':

                        # Convert the regular post to a photo post and
                        # create the photo caption once
                        if self.posts[i_post]['type'] == "text":
                            self.posts[i_post]['type'] = 'photo'
                            self.posts[i_post]['caption'] = (
                                '<p>' + self.posts[i_post]['title'] + '</p>'
                                + self.posts[i_post]['body'])
                            self.posts[i_post]['photos'] = []
                            self.posts[i_post]['converted'] = True
                            self.logger.info("{0} converted regular post to photo post ({1})".format(
                                self.subdomain, i_post))

                        # Append the photo URL
                        self.posts[i_post]['photos'].append({})
                        self.posts[i_post]['photos'][-1]['caption'] = ""
                        self.posts[i_post]['photos'][-1]['alt_sizes'] = []
                        self.posts[i_post]['photos'][-1]['alt_sizes'].append({})
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['width'] = 0
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['height'] = 0
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['url'] = element.get('src')
