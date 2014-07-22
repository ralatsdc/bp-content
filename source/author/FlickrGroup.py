# -*- coding: utf-8 -*-

# Standard library imports
import datetime
import logging
import math
import os
import pickle
import time

# Third-party imports
import flickrapi

# Local imports
from utility.AuthorUtility import AuthorUtility

class FlickrGroup(object):
    """Represents a group on Flickr by their creative output.

    """
    def __init__(self, blu_pen_author, source_word_str, group_id, content_dir,
                 requested_dt=datetime.datetime.utcnow(), max_photos=100,
                 api_key="71ae5bd2b331d44649161f6d3ff7e6b6", api_secret="45f1be4bd59f9155",
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a FlickrGroup instance.

        """
        # Process the source word string to create log and path
        # strings, and assign input argument attributes
        self.blu_pen_author = blu_pen_author
        self.author_utility = AuthorUtility()
        (self.source_log,
         self.source_path,
         self.source_header,
         self.source_label,
         self.source_type,
         self.source_word) = self.author_utility.process_source_words(source_word_str)
        self.group_id = group_id
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, group_id + ".pkl")
        self.requested_dt = requested_dt
        self.max_photos = max_photos
        self.api_key = api_key
        self.api_secret = api_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        # Initialize created attributes
        self.photos = []
        self.created_dt = []
        self.content_set = False

        # Create an API
        self.api = flickrapi.FlickrAPI(self.api_key)

        # Create a logger
        self.logger = logging.getLogger(u"FlickrGroup")

        # Check input arguments
        if not self.source_type == u'@':
            err_msg = u"{0} can only search by group (@)".format(
                self.source_log)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))
        if not type(self.source_word) == unicode:
            err_msg = u"{0} only one source word accepted as type unicode".format(
                self.source_log)
            self.logger.error(err_msg)
            raise Exception(err_msg.encode('utf-8'))

    def set_photos(self, do_purge=False):
        """Gets recent photos for this group from Flickr, and parses
        attributes and values.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the FlickrGroup pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0} getting photos for {1}".format(
                self.source_log, self.group_id))

            # Get photos by group identifier
            photos = self.get_photos_by_source(self.group_id)
            if photos == None:
                photos = []

            # Parse attributes and values
            self.photos = []
            iPh = -1
            for photo in photos:
                iPh += 1
                if iPh > self.max_photos - 1:
                    break
                self.photos.append({})

                # Usual
                self.photos[-1]['id'] = photo.get("id")
                self.photos[-1]['owner'] = photo.get("owner")
                self.photos[-1]['secret'] = photo.get("secret")
                self.photos[-1]['server'] = photo.get("server")
                self.photos[-1]['farm'] = photo.get("farm")
                self.photos[-1]['title'] = photo.get("title")
                self.photos[-1]['ispublic'] = photo.get("ispublic")
                self.photos[-1]['isfriend'] = photo.get("isfriend")
                self.photos[-1]['isfamily'] = photo.get("isfamily")
                self.photos[-1]['ownername'] = photo.get("ownername")
                self.photos[-1]['dateadded'] = photo.get("dateadded")

                # Extras
                self.photos[-1]['dateupload'] = photo.get("dateupload")
                self.photos[-1]['datetaken'] = photo.get("datetaken")
                self.photos[-1]['latitude'] = photo.get("latitude")
                self.photos[-1]['longitude'] = photo.get("longitude")
                self.photos[-1]['tags'] = photo.get("tags")
                self.photos[-1]['url_m'] = photo.get("url_m")

            self.content_set = True

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_photos_by_source(self, group_id , per_page=500, page=1):
        """Makes multiple attempts to get photos by source, sleeping
        before attempts.

        Typical values. Currently returned fields are:
        ... id
        ... owner
        ... title
        ... secret
        ... server
        ... ispublic
        ... isfriend
        ... isfamily
        ... ownername
        ... dateadded

        Extra values. A comma-delimited list of extra information to
        fetch for each returned record. Currently supported fields
        are:
        ... description
        ... license
        ... date_upload
        ... date_taken
        ... owner_name
        ... icon_server
        ... original_format
        ... last_update
        ... geo
        ... tags
        ... machine_tags
        ... o_dims
        ... views
        ... media
        ... path_alias
        ... url_sq
        ... url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o

        URL formats:
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{secret}.jpg
	...... or
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{secret}_[mstzb].jpg
	...... or
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{o-secret}_o.(jpg|gif|png)

        Size suffixes:
        ... s small square 75x75
        ... t thumbnail, 100 on longest side
        ... m small, 240 on longest side
        ... - medium, 500 on longest side
        ... z medium 640, 640 on longest side
        ... b large, 1024 on longest side*
        ... o original image, either a jpg, gif or png, depending on source format

        Privacy_filter values:
        ... 1 public photos
        ... 2 private photos visible to friends
        ... 3 private photos visible to family
        ... 4 private photos visible to friends & family
        ... 5 completely private photos

        Maximum per_page:
        ... 500

        Media types:
        ... all (default)
        ... photos
        ... videos 

        """
        photos = None

        # Make multiple attempts to get photos by source
        iAttempts = 0
        while photos is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Make attempt to get photos by source
            try:
                photos = self.api.groups_pools_getPhotos(
                    group_id=group_id,
                    extras='date_upload, date_taken, geo, tags, url_m').find('photos').findall('photo')
                self.logger.info(u"{0} collected {1} photos for {2}".format(
                    self.source_log, len(photos), group_id))
            except Exception as exc:
                photos = None
                self.logger.warning(u"{0} couldn't get photos for {1}: {2}".format(
                    self.source_log, group_id, exc))

        return photos

    def download_photos(self):
        """Download all photos by this group from Flickr.

        """
        for photo in self.photos:
            photo_url = photo['url_m']
            if photo_url != None:
                head, tail = os.path.split(photo_url)
                photo_file_name = os.path.join(self.content_dir, tail)
                photo['file_name'] = photo_file_name
                if not os.path.exists(photo_file_name):
                    try:
                        self.author_utility.download_file(photo_url, photo_file_name)
                        self.logger.info(u"{0} photo downloaded to file {1}".format(
                            self.source_log, photo_file_name))
                    except Exception as exc:
                        self.logger.warning(u"{0} could not download photo to file {1}".format(
                            self.source_log, photo_file_name))
                else:
                    self.logger.info(u"{0} photo already downloaded to file {1}".format(
                        self.source_log, photo_file_name))

    def dump(self, pickle_file_name=None):
        """Dump FlickrGroup attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_log'] = self.source_log
        p['source_path'] = self.source_path
        p['source_header'] = self.source_header
        p['source_label'] = self.source_label
        p['source_type'] = self.source_type
        p['source_word'] = self.source_word

        p['group_id'] = self.group_id
        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['max_photos'] = self.max_photos
        p['api_key'] = self.api_key
        p['api_secret'] = self.api_secret
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['photos'] = self.photos
        p['created_dt'] = self.created_dt
        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} photos to {2}".format(
                self.source_log, len(self.photos), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load FlickrGroup attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_log = p['source_log']
        self.source_path = p['source_path']
        self.source_header = p['source_header']
        self.source_label = p['source_label']
        self.source_type = p['source_type']
        self.source_word = p['source_word']

        self.group_id = p['group_id']
        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.max_photos = p['max_photos']
        self.api_key = p['api_key']
        self.api_secret = p['api_secret']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.photos = p['photos']
        self.created_dt = p['created_dt']
        self.content_set = p['content_set']

        self.logger.info(u"{0} loaded {1} photos from {2}".format(
                self.source_log, len(self.photos), pickle_file_name))

        pickle_file.close()
