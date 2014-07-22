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

class FlickrAuthor(object):
    """Represents an author on Flickr by their creative output.

    """
    def __init__(self, blu_pen_author, source_word_str, content_dir,
                 requested_dt=datetime.datetime.utcnow(), max_photosets=100,
                 api_key='71ae5bd2b331d44649161f6d3ff7e6b6', api_secret='45f1be4bd59f9155',
                 number_of_api_attempts=1, seconds_between_api_attempts=1):
        """Constructs a FlickrAuthor instance.

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
        self.content_dir = os.path.join(content_dir, self.source_path)
        self.pickle_file_name = os.path.join(self.content_dir, self.source_path + ".pkl")
        self.requested_dt = requested_dt
        self.max_photosets = max_photosets
        self.api_key = api_key
        self.api_secret = api_secret
        self.number_of_api_attempts = number_of_api_attempts
        self.seconds_between_api_attempts = seconds_between_api_attempts

        # Initialize created attributes
        self.photosets = []
        self.created_dt = []
        self.content_set = False

        # Create an API
        self.api = flickrapi.FlickrAPI(self.api_key)

        # Create a logger
        self.logger = logging.getLogger("FlickrAuthor")

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

        # Find the user identifier associated with the user name
        try:
            user_xml = self.api.people_findByUsername(username=self.source_word)
            self.user_id = user_xml.find("user").get("nsid")
            self.logger.info(u"{0} found {1} for {2}".format(
                self.source_log, self.user_id, self.source_word))
        except Exception as exc:
            self.logger.warning(u"{0} could not get user XML or parse user ID".format(
                self.source_log))
            self.user_id = None

    def set_photosets(self, do_purge=False):
        """Gets recent photosets for this author from Flickr, and
        parses attributes and values.

        """
        # Create content directory, if it does not exist
        if not os.path.exists(self.content_dir):
            os.makedirs(self.content_dir)

        # Remove pickle file, if requested
        if do_purge and os.path.exists(self.pickle_file_name):
            os.remove(self.pickle_file_name)

        # Create and dump, or load, the FlickrAuthor pickle
        if not os.path.exists(self.pickle_file_name):
            self.logger.info(u"{0} getting photosets for {1}".format(
                self.source_log, self.user_id))

            # Get photosets list
            photosets = self.get_photosets_by_source(self.user_id)

            # Parse attributes and values
            self.photosets = []
            iPS = -1
            for photoset in photosets:
                iPS += 1
                if iPS > self.max_photosets - 1:
                    break
                self.photosets.append({})
                self.photosets[-1]['id'] = photoset.get("id")
                self.photosets[-1]['primary'] = photoset.get("primary")
                self.photosets[-1]['secret'] = photoset.get("secret")
                self.photosets[-1]['server'] = photoset.get("server")
                self.photosets[-1]['photos'] = photoset.get("photos")
                self.photosets[-1]['farm'] = photoset.get("farm")
                self.photosets[-1]['title'] = photoset.findtext("title")
                self.photosets[-1]['description'] = photoset.findtext("description")

            self.content_set = True

            # Dumps attributes pickle
            self.dump()

        else:

            # Load attributes pickle
            self.load()

    def get_photosets_by_source(self, user_id):
        """Makes multiple attempts to get photosets by source,
        sleeping before attempts.

        """
        photosets = None

        # Makes multiple attempts to get photosets by source
        iAttempts = 0
        while photosets is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Makes attempt to get photosets by source
            try:
                photosets = self.api.photosets_getList(user_id=user_id).find("photosets").findall("photoset")
                self.logger.info(u"{0} collected {1} photosets for {2}".format(
                    self.source_log, len(photosets), user_id))
            except Exception as exc:
                photosets = None
                self.logger.warning(u"{0} couldn't get photosets for {1}: {2}".format(
                    self.source_log, user_id, exc))

        return photosets

    def set_photos(self, per_page=500, page=1):
        """Gets recent photos for each photoset for this author, then
        parses attributes.
        
        """
        # Consider each photo set
        iPS = -1
        for photoset in self.photosets:
            iPS += 1

            # Get photos in the current photoset
            photos = self.get_photos_in_photoset(photoset)
            if photos is None:
                continue

            # Parse attributes
            self.photosets[iPS]['photos'] = []
            iPh = 0
            for photo in photos:
                iPh += 1
                if iPh > 10:
                    break
                self.photosets[iPS]['photos'].append({})
                
                # Usual
                self.photosets[iPS]['photos'][-1]['id'] = photo.get("id")
                self.photosets[iPS]['photos'][-1]['secret'] = photo.get("secret")
                self.photosets[iPS]['photos'][-1]['server'] = photo.get("server")
                self.photosets[iPS]['photos'][-1]['title'] = photo.get("title")
                self.photosets[iPS]['photos'][-1]['isprimary'] = photo.get("isprimary")

                # Extras
                self.photosets[iPS]['photos'][-1]['dateupload'] = photo.get("dateupload")
                self.photosets[iPS]['photos'][-1]['datetaken'] = photo.get("datetaken")
                self.photosets[iPS]['photos'][-1]['latitude'] = photo.get("latitude")
                self.photosets[iPS]['photos'][-1]['longitude'] = photo.get("longitude")
                self.photosets[iPS]['photos'][-1]['tags'] = photo.get("tags")
                self.photosets[iPS]['photos'][-1]['url_m'] = photo.get("url_m")

        if iPS > -1:
            self.created_dt.append(
                datetime.strptime(self.photosets[0]['photos'][0]['datetaken'], "%Y-%m-%d %H:%M:%S"))
            self.created_dt.append(
                datetime.strptime(self.photosets[-1]['photos'][-1]['datetaken'], "%Y-%m-%d %H:%M:%S"))

    def get_photos_in_photoset(self, photoset, per_page=500, page=1):
        """Makes multiple attempts to get photos in the specified
        photoset, sleeping before attempts.

        Typical values. Currently returned fields are:
        ... TODO: Complete

        Extra values. A comma-delimited list of extra information to
        fetch for each returned record. Currently supported fields
        are:
        ... license
        ... date_upload, date_taken
        ... owner_name
        ... icon_server
        ... original_format
        ... last_update
        ... geo
        ... tags, machine_tags
        ... o_dims
        ... views
        ... media
        ... path_alias
        ... url_sq, url_t, url_s, url_m, url_o

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

        Valid media types:
        ... all (default)
        ... photos
        ... videos 

        """
        photos = None

        # Makes multiple attempts to get photos in the specified
        # photoset
        iAttempts = 0
        while photos is None and iAttempts < self.number_of_api_attempts:
            iAttempts += 1

            # Sleep before attempt
            seconds_between_api_attempts = self.seconds_between_api_attempts * math.pow(2, iAttempts - 1)
            self.logger.info(u"{0} sleeping for {1} seconds".format(
                self.source_log, seconds_between_api_attempts))
            time.sleep(seconds_between_api_attempts)

            # Makes attempt to get photos in the specified photoset
            try:
                photos = self.api.photosets_getPhotos(
                    photoset_id=photoset['id'],
                    extras='date_upload, date_taken, geo, tags, url_m',
                    privacy_filter=1, per_page=per_page, page=page, media='photo').find('photoset').findall('photo')
                self.logger.info(u"{0} collected {1} photos".format(
                    self.source_log, len(photos)))
            except Exception as exc:
                photos = None
                self.logger.warning(u"{0} couldn't get photos for {1}: {2}".format(
                    self.source_log, photoset['id'], exc))

        return photos

    def download_photos(self):
        """Download all photos by this author from Flickr.

        """
        iPS = -1
        for photoset in self.photosets:
            iPS += 1
            iPh = -1
            for photo in photoset['photos']:
                iPh += 1
                photo_url = photo['url_m']
                if photo_url != None:
                    head, tail = os.path.split(photo_url)
                    photo_file_name = os.path.join(self.content_dir, tail)
                    self.photosets[iPS]['photos'][iPh]['file_name'] = photo_file_name
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
        """Dump FlickrAuthor attributes pickle.

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

        p['user_id'] = self.user_id
        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['max_photosets'] = self.max_photosets
        p['api_key'] = self.api_key
        p['api_secret'] = self.api_secret
        p['number_of_api_attempts'] = self.number_of_api_attempts
        p['seconds_between_api_attempts'] = self.seconds_between_api_attempts

        p['photosets'] = self.photosets
        p['created_dt'] = self.created_dt
        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info(u"{0} dumped {1} photosets to {2}".format(
                self.source_log, len(self.photosets), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load FlickrAuthor attributes pickle.

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

        self.user_id = p['user_id']
        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.max_photosets = p['max_photosets']
        self.api_key = p['api_key']
        self.api_secret = p['api_secret']
        self.number_of_api_attempts = p['number_of_api_attempts']
        self.seconds_between_api_attempts = p['seconds_between_api_attempts']

        self.photosets = p['photosets']
        self.created_dt = p['created_dt']
        self.content_set = p['content_set']

        self.logger.info(u"{0} loaded {1} photosets from {2}".format(
                self.source_log, len(self.photosets), pickle_file_name))

        pickle_file.close()
