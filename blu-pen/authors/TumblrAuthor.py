# -*- coding: utf-8 -*-

# Standard library imports
import datetime
import hashlib
import imghdr
import logging
import math
import os
import pickle
import shutil
import time
import urlparse

# Third-party imports
from lxml.html import soupparser
import pytumblr

# Local imports
from utility.AuthorsUtility import AuthorsUtility

class TumblrAuthor:
    """Represents an author on Tumblr by their creative
    output.

    """
    def __init__(self, blu_pen_author, subdomain, content_dir,
                 requested_dt=datetime.datetime.now(), max_posts=100,
                 consumer_key="7c3XQwWIUJS9hjJ9EPzhx2qlySQ5J2sIRgXRN89Ld03AGtK1KP",
                 secret_key="R8Y1Qj7wODcorDid3A24Ct1bfUg0wGoT9iB4n2GgXwKcTb6csb",
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a TumblrAuthor given a subdomain.

        """
        # Assign input argument attributes
        self.blu_pen_author = blu_pen_author
        self.authors_utility = AuthorsUtility()
        self.subdomain = subdomain
        self.content_dir = os.path.join(content_dir, self.subdomain)
        self.pickle_file_name = os.path.join(self.content_dir, self.subdomain + ".pkl")
        self.requested_dt = requested_dt
        self.max_posts = max_posts
        self.consumer_key = consumer_key
        self.secret_key = secret_key
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts
        
        # Initialize created attributes
        self.info = None
        self.posts = []
        
        # Create an API
        self.client = pytumblr.TumblrRestClient(self.consumer_key, self.secret_key)

        # Create a logger
        self.logger = logging.getLogger(__name__)
        
    def set_posts(self, limit=20, do_purge=False):
        """Gets recent posts by this author from Tumblr.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the TumblrSources pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0} finding posts for {1}".format(
                self.subdomain, self.subdomain))

            # Get blog information, including the total number of posts
            self.info = self.get_info_by_subdomain(self.subdomain)
            if self.info is None:
                return
        
            # Get sets of posts, containing the maximum number of posts,
            # until all posts are collected
            while len(self.posts) < self.max_posts:
                offset = len(self.posts)
                self.posts.extend(self.get_posts_by_subdomain(self.subdomain, limit=limit, offset=offset))
                self.logger.info(u"{0} collected {1} total posts for {2}".format(
                    self.subdomain, len(self.posts), self.subdomain))

            # Put posts in chronological order
            def convert_string_datetime(post):
                posted_dt = datetime.datetime.fromtimestamp(post["timestamp"])
                return posted_dt
            self.posts.sort(key=convert_string_datetime)

            # Convert regular posts containing images to photo posts.
            self.convert_text_posts()

            # Dumps attributes pickle
            self.dump()

        else:
            
            # Load attributes pickle
            self.load()

    def get_info_by_subdomain(self, subdomain):
        """Makes multiple attempts to get info by subdomain, sleeping
        before attempts.

        """
        info = None

        # Make multiple attempts to get info by subdomain
        iAttempts = 0
        while info is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.subdomain, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make attempt to get info by subdomain
            try:
                info = self.client.blog_info(subdomain)['blog']
                self.logger.info(u"{0} collected info for {1}".format(
                    self.subdomain, subdomain))
            except Exception as exc:
                info = None
                self.logger.warning(u"{0} couldn't get info for {1}: {2}".format(
                    self.subdomain, subdomain, exc))

        return info

    def get_posts_by_subdomain(self, subdomain, limit=20, offset=0):
        """Makes multiple attempts to get posts by subdomain, sleeping
        before attempts. The maximum number of posts is requested.

        """
        posts = None

        # Make multiple attempts to get posts by subdomain
        iAttempts = 0
        while posts is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.subdomain, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make attempt to get posts by subdomain
            try:
                posts = self.client.posts(subdomain, limit=limit, offset=offset)['posts']
                self.logger.info(u"{0} collected {1} posts for {2}".format(
                    self.subdomain, len(posts), subdomain))
            except Exception as exc:
                posts = None
                self.logger.warning(u"{0} couldn't get posts for {1}: {2}".format(
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
                    head, tail = os.path.split(urlparse.urlparse(photo_url).path)
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
                            self.authors_utility.download_file(photo_url, photo_file_name)
                            self.logger.info(u"{0} downloaded photo to file {1}".format(
                                self.subdomain, photo_file_name))

                        except Exception as exc:
                            self.logger.warning(u"{0} could not download photo at url {1}".format(
                                self.subdomain, photo_url))

                            # Remove the photo file
                            if os.path.exists(photo_file_name):
                                os.remove(photo_file_name)
                                self.logger.info(u"{0} removed photo file {1}".format(
                                    self.subdomain, photo_file_name))
                            photo_file_name = ""

                    else:
                        self.logger.info(u"{0} photo already downloaded to file {1}".format(
                            self.subdomain, photo_file_name))

                    # Recreate the photo file name, if the extension
                    # was not defined previously
                    if ext == ".tmp" and not photo_file_name == "":
                        try:
                            ext = "." + imghdr.what(photo_file_name)
                            shutil.move(photo_file_name, photo_file_name.replace(".tmp", ext))
                            photo_file_name = photo_file_name.replace(".tmp", ext)
                            self.logger.info(u"{0} moved photo file to {1}".format(
                                self.subdomain, photo_file_name))

                        except Exception as exc:
                            self.logger.info(u"{0} could not move photo file {1}".format(
                                self.subdomain, photo_file_name))

                            # Remove the photo file
                            if os.path.exists(photo_file_name):
                                os.remove(photo_file_name)
                                self.logger.info(u"{0} removed photo file {1}".format(
                                    self.subdomain, photo_file_name))
                            photo_file_name = ""

                    # Assign the photo file name
                    self.posts[i_post]['photos'][i_photo]['photo-file-name'] = photo_file_name

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
                            self.posts[i_post]['caption'] = ""
                            if not self.posts[i_post]['title'] is None:
                                self.posts[i_post]['caption'] += '<p>' + self.posts[i_post]['title'] + '</p>'
                            if not self.posts[i_post]['body'] is None:
                                self.posts[i_post]['caption'] += self.posts[i_post]['body']
                            self.posts[i_post]['photos'] = []
                            self.posts[i_post]['converted'] = True
                            self.logger.info(u"{0} converted regular post to photo post ({1})".format(
                                self.subdomain, i_post))

                        # Append the photo URL
                        self.posts[i_post]['photos'].append({})
                        self.posts[i_post]['photos'][-1]['caption'] = ""
                        self.posts[i_post]['photos'][-1]['alt_sizes'] = []
                        self.posts[i_post]['photos'][-1]['alt_sizes'].append({})
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['width'] = 0
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['height'] = 0
                        self.posts[i_post]['photos'][-1]['alt_sizes'][-1]['url'] = element.get('src')

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
        p['max_posts'] = self.max_posts
        p['consumer_key'] = self.consumer_key
        p['secret_key'] = self.secret_key
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['posts'] = self.posts
        
        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} posts to {2}".format(
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
        self.max_posts = p['max_posts']
        self.consumer_key = p['consumer_key']
        self.secret_key = p['secret_key']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.posts = p['posts']

        self.logger.info(u"{0} loaded {1} posts from {2}".format(
            self.subdomain, len(self.posts), pickle_file_name))

        pickle_file.close()
