# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime
import logging
import os
import pickle

# Third-party imports
import flickrapi

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility

class FlickrAuthor:
    """Represents an author on Flickr by their creative output.

    """
    def __init__(self, blu_pen, username, content_dir, requested_dt=datetime.utcnow(),
                 max_photo_sets=100, api_key='71ae5bd2b331d44649161f6d3ff7e6b6', api_secret='45f1be4bd59f9155'):
        """Constructs a FlickrAuthor instance.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utility = BluePeninsulaUtility()

        self.username = username
        self.content_dir = content_dir
        self.requested_dt = requested_dt
        self.max_photo_sets = max_photo_sets
        self.api_key = api_key
        self.api_secret = api_secret

        self.pickle_file_name = os.path.join(self.content_dir, self.username + ".pkl")

        self.photosets = []
        self.created_dt = []
        self.content_set = False
        self.api = flickrapi.FlickrAPI(self.api_key)
        self.logger = logging.getLogger(__name__)
        try:
            user_xml = self.api.people_findByUsername(username=self.username)
            self.user_id = user_xml.find("user").get("nsid")
        except Exception as exc:
            self.logger.error("{0} could not get user XML or parse user ID".format(self.username))
            self.user_id = None

    def set_photosets_as_recent(self):
        """Gets recent photosets for this author from Flickr and
        parses attributes and values.

        """
        # Get photosets
        try:
            photosets_list_xml = self.api.photosets_getList(user_id=self.user_id)
        except Exception as exc:
            self.logger.error("{0} could not get photosets list XML for {1}".format(
                    self.username, self.user_id))

        # Parse attributes and values
        self.photosets = []
        iPS = 0
        try:
            photoset_xml_list = photosets_list_xml.find("photosets").findall("photoset")
        except Exception as exc:
            self.logger.error("{0} could not get list of photoset XML for {1}".format(
                    self.username, self.user_id))
            return
        for photoset_xml in photoset_xml_list:
            iPS += 1
            if iPS > self.max_photo_sets:
                break
            self.photosets.append({})
            self.photosets[-1]['id'] = photoset_xml.get("id")
            self.photosets[-1]['primary'] = photoset_xml.get("primary")
            self.photosets[-1]['secret'] = photoset_xml.get("secret")
            self.photosets[-1]['server'] = photoset_xml.get("server")
            self.photosets[-1]['photos'] = photoset_xml.get("photos")
            self.photosets[-1]['farm'] = photoset_xml.get("farm")
            self.photosets[-1]['title'] = photoset_xml.findtext("title")
            self.photosets[-1]['description'] = photoset_xml.findtext("description")

        self.content_set = True

    def set_photos_as_recent(self, per_page=500, page=1):
        """Gets recent photos for each photoset for this author, then
        parses attributes.

        Valid extras value:
        ... A comma-delimited list of extra information to fetch for
        each returned record. Currently supported fields are:
        ...... license
        ...... date_upload, date_taken
        ...... owner_name
        ...... icon_server
        ...... original_format
        ...... last_update
        ...... geo
        ...... tags, machine_tags
        ...... o_dims
        ...... views
        ...... media
        ...... path_alias
        ...... url_sq, url_t, url_s, url_m, url_o

        The URL takes the following format:
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{secret}.jpg
	...... or
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{secret}_[mstzb].jpg
	...... or
        ... http://farm{farm-id}.static.flickr.com/{server-id}/{id}_{o-secret}_o.(jpg|gif|png)

        Valid size suffixes:
        ... s small square 75x75
        ... t thumbnail, 100 on longest side
        ... m small, 240 on longest side
        ... - medium, 500 on longest side
        ... z medium 640, 640 on longest side
        ... b large, 1024 on longest side*
        ... o original image, either a jpg, gif or png, depending on source format

        Valid privacy_filter values:
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
        # Consider each photo set
        iPS = -1
        for photo_set in self.photosets:
            iPS += 1

            # Get photos
            try:
                photoset_xml = self.api.photosets_getPhotos(
                    photoset_id=photo_set['id'],
                    extras='date_upload, date_taken, geo, tags, url_m',
                    privacy_filter=1, per_page=per_page, page=page, media='photo')
            except Exception as exc:
                self.logger.error("{0} could not get photoset XML for {1}".format(
                        self.username, photo_set['id']))

            # Parse attributes
            self.photosets[iPS]['photos'] = []
            iPh = 0
            for photo_xml in photoset_xml.find('photoset').findall('photo'):
                iPh += 1
                if iPh > 10:
                    break
                self.photosets[iPS]['photos'].append({})
                
                # Usual
                self.photosets[iPS]['photos'][-1]['id'] = photo_xml.get("id")
                self.photosets[iPS]['photos'][-1]['secret'] = photo_xml.get("secret")
                self.photosets[iPS]['photos'][-1]['server'] = photo_xml.get("server")
                self.photosets[iPS]['photos'][-1]['title'] = photo_xml.get("title")
                self.photosets[iPS]['photos'][-1]['isprimary'] = photo_xml.get("isprimary")

                # Extras
                self.photosets[iPS]['photos'][-1]['dateupload'] = photo_xml.get("dateupload")
                self.photosets[iPS]['photos'][-1]['datetaken'] = photo_xml.get("datetaken")
                self.photosets[iPS]['photos'][-1]['latitude'] = photo_xml.get("latitude")
                self.photosets[iPS]['photos'][-1]['longitude'] = photo_xml.get("longitude")
                self.photosets[iPS]['photos'][-1]['tags'] = photo_xml.get("tags")
                self.photosets[iPS]['photos'][-1]['url_m'] = photo_xml.get("url_m")

        if iPS > -1:
            self.created_dt.append(
                datetime.strptime(self.photosets[0]['photos'][0]['datetaken'], "%Y-%m-%d %H:%M:%S"))

            self.created_dt.append(
                datetime.strptime(self.photosets[-1]['photos'][-1]['datetaken'], "%Y-%m-%d %H:%M:%S"))

    def download_photos(self):
        """Download all photos by this author from Flickr.

        """
        iPS = -1
        for photo_set in self.photosets:
            iPS += 1
            iPh = -1
            for photo in photo_set['photos']:
                iPh += 1
                photo_url = photo['url_m']
                if photo_url != None:
                    head, tail = os.path.split(photo_url)
                    photo_file_name = os.path.join(self.content_dir, tail)
                    self.blu_pen_utility.download_file(photo_url, photo_file_name)
                    self.photosets[iPS]['photos'][iPh]['file_name'] = photo_file_name
                    self.logger.info("{0} downloaded photo to file {1}".format(
                            self.username, photo_file_name))

    def dump(self, pickle_file_name=None):
        """Dump FlickrAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['username'] = self.username
        p['content_dir'] = self.content_dir
        p['requested_dt'] = self.requested_dt
        p['max_photo_sets'] = self.max_photo_sets
        p['api_key'] = self.api_key
        p['api_secret'] = self.api_secret

        p['photosets'] = self.photosets
        p['created_dt'] = self.created_dt
        p['content_set'] = self.content_set

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped {1} photosets to {2}".format(
                self.username, len(self.photosets), pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load FlickrAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.username = p['username']
        self.content_dir = p['content_dir']
        self.requested_dt = p['requested_dt']
        self.max_photo_sets = p['max_photo_sets']
        self.api_key = p['api_key']
        self.api_secret = p['api_secret']

        self.photosets = p['photosets']
        self.created_dt = p['created_dt']
        self.content_set = p['content_set']

        self.logger.info("{0} loaded {1} photosets from {2}".format(
                self.username, len(self.photosets), pickle_file_name))

        pickle_file.close()
