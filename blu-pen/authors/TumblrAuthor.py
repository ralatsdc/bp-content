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
from urlparse import urlparse

# Third-party imports
from lxml.html import soupparser
import tumblr

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class TumblrAuthor:
    """Represents an author on Tumblr by their creative
    output. Authors are selected by their subdomain.

    """
    def __init__(self, blu_pen, subdomain, content_dir, requested_dt=datetime.now()):
        """Constructs a TumblrAuthor given a subdomain.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utility = BluePeninsulaUtility()

        self.subdomain = subdomain
        self.content_dir = content_dir
        self.requested_dt = requested_dt

        self.pickle_file_name = os.path.join(self.content_dir, self.subdomain + ".pkl")
        
        self.posts = []
        
        self.api = tumblr.Api(subdomain + ".tumblr.com")
        self.logger = logging.getLogger(__name__)
        
    def set_posts_as_recent(self):
        """Gets recent posts by this author from Tumblr.

        """
        # Get posts by subdomain
        self.posts = get_posts_by_subdomain(subdomain)
        if self.posts is None:
            return

        # Put posts in chronological order
        def convert_string_datetime(post):
            posted_dt = datetime.fromtimestamp(post["unix-timestamp"])
            return posted_dt
        self.posts.sort(key=convert_string_datetime)
        
        # Convert regular posts containing images to photo posts.
        self.convert_image_posts()

    def get_posts_by_subdomain(self, subdomain):
        """Makes multiple attempts to get posts by subdomain, sleeping
        before attempts.

        """
        posts = []

        # Make multiple attempts
        exc = None
        iAttempts = 0
        while length(posts) == 0 and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep longer before each attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info("{0} sleeping for {1} seconds".format(
                self.subdomain, seconds_between_api_attempts))
            sleep(seconds_between_api_attempts)

            # Attempt to get poste by subdomain
            try:
                posts_iter = self.api.read()
                for post in posts_iter:
                    posts.append(post)
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
            if n_photos == 0:
                n_photos = 1
            for i_photo in range(n_photos):

                # Assign the photo URL
                if n_photos == 1:
                    photo_url = self.posts[i_post]['photo-url-500']
                else:
                    photo_url = self.posts[i_post]['photos'][i_photo]['photo-url-500']
                print "photo_url: ", photo_url

                if photo_url != None:
                    
                    # Create the photo file name
                    head, tail = os.path.split(urlparse(photo_url).path)
                    print "head: ", head
                    print "tail: ", tail
                    root, ext = os.path.splitext(tail)
                    print "root: ", root
                    print "ext: ", ext
                    if (len(ext) == 0
                        or not ext.lower() in ['.rgb', '.gif', '.pbm', '.pgm', '.ppm', '.tiff', '.tif',
                                               '.rast', '.xbm', '.jpeg', '.jpg', '.bmp', '.png']):
                        ext = ".tmp"
                    print "ext: ", ext
                    if self.posts[i_post]['converted']:
                        tail = hashlib.sha224(photo_url).hexdigest() + ext
                    print "tail: ", tail
                    photo_file_name = os.path.join(self.content_dir, tail)
                    print "photo_file_name: ", photo_file_name

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
                    if n_photos == 1:
                        self.posts[i_post]['photo-file-name'] = photo_file_name
                    else:
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

        p['posts'] = self.posts
        
        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped {1} posts to {2}".format(
            self.subdomain, length(self.posts), pickle_file_name))

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

        self.posts = p['posts']

        self.logger.info("{0} loaded {1} posts from {2}".format(
            self.subdomain, length(self.posts), pickle_file_name))

        pickle_file.close()

    def convert_image_posts(self):
        """Convert a regular post containing images to a photo post.

        """
        # Consider each post
        n_posts = len(self.posts)
        for i_post in range(n_posts):
            self.posts[i_post]['converted'] = False

            # Convert regular posts only
            if self.posts[i_post]['type'] == "regular":

                # Parse the HTML content and consider each element
                root = soupparser.fromstring(self.posts[i_post]['regular-body'])
                for element in root.iter():

                    # Process image elements only
                    if element.tag == 'img':

                        # Convert the regular post to a photo post and
                        # create the photo caption once
                        if self.posts[i_post]['type'] == "regular":
                            self.posts[i_post]['type'] = 'photo'
                            self.posts[i_post]['photo-caption'] = (
                                '<p>' + self.posts[i_post]['regular-title'] + '</p>'
                                + self.posts[i_post]['regular-body'])
                            self.posts[i_post]['photos'] = []
                            self.posts[i_post]['converted'] = True
                            self.logger.info("{0} converted regular post to photo post ({1})".format(
                                self.subdomain, i_post))

                        # Append the photo URL
                        self.posts[i_post]['photos'].append({})
                        self.posts[i_post]['photos'][-1]['photo-url-500'] = element.get('src')

            # Assign photo URL as a post, rather than a photo, key, if
            # a photo post, and there is only one photo
            if self.posts[i_post]['type'] == "photo" and len(self.posts[i_post]['photos']) == 1:
                self.posts[i_post]['photo-url-500'] = self.posts[i_post]['photos'][0]['photo-url-500']
                self.posts[i_post]['photos'] = []
