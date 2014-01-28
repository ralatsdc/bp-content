# -*- coding: utf-8 -*-

# Standard library imports
import codecs
from datetime import datetime
import logging
import math
import os
import pickle
import random
import uuid
import xml

# Third-party imports
import flickrapi

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility
from FlickrBoulat import FlickrBoulat

class FlickrAuthor:
    """Represents an author on Flickr by their creative output. Books
    are named after documentary photographers.

    """
    def __init__(self, blu_pen, username, content_dir, max_photo_sets=100,
                 api_key='71ae5bd2b331d44649161f6d3ff7e6b6', api_secret='45f1be4bd59f9155'):
        """Constructs a FlickrAuthor given a username.

        """
        self.blu_pen = blu_pen
        self.username = username
        self.content_dir = content_dir
        self.pickle_file_name = os.path.join(self.content_dir, self.username + ".pkl")
        self.max_photo_sets = max_photo_sets
        self.api_key = api_key
        self.api_secret = api_secret

        self.api = flickrapi.FlickrAPI(self.api_key)
        
        self.logger = logging.getLogger(__name__)
        try:
            user_xml = self.api.people_findByUsername(username=self.username)
            self.user_id = user_xml.find("user").get("nsid")
        except Exception as exc:
            self.logger.error("{0} could not get user XML or parse user ID".format(self.username))
            self.user_id = None

        self.photosets = []

        self.created_dt = []

        self.profile_image_file_name = None
        self.background_image_file_name = None

        self.blu_pen_utility = BluePeninsulaUtility()

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
        for photoset_xml in photosets_list_xml.find("photosets").findall("photoset"):
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

        photo_url = self.photosets[0]['photos'][0]['url_m']
        head, tail = os.path.split(photo_url)
        self.profile_image_file_name = os.path.join(self.content_dir, tail)

        photo_url = self.photosets[-1]['photos'][-1]['url_m']
        head, tail = os.path.split(photo_url)
        self.background_image_file_name = os.path.join(self.content_dir, tail)

    def dump(self, pickle_file_name=None):
        """Dump FlickrAuthor attributes pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['photosets'] = self.photosets

        p['created_dt'] = self.created_dt

        p['profile_image_file_name'] = self.profile_image_file_name
        p['background_image_file_name'] = self.background_image_file_name

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

        self.photosets = p['photosets']

        self.created_dt = p['created_dt']

        self.profile_image_file_name = p['profile_image_file_name']
        self.background_image_file_name = p['background_image_file_name']

        self.logger.info("{0} loaded {1} photosets from {2}".format(
                self.username, len(self.photosets), pickle_file_name))

        pickle_file.close()

    def write_boulat_contents(self, file_name, empty_pages=int(0)):
        """Write ConTeXt file to produce (Alexandra) Boulat contents.
        
        """
        # Initialize FlickrBoulat
        flickr_boulat = FlickrBoulat(self.profile_image_file_name, self.background_image_file_name)

        # Write contents set up TeX
        out = codecs.open(file_name, mode='w', encoding='utf-8', errors='ignore')
        out.write(flickr_boulat.get_contents_set_up_tex(self.username))

        # Write front matter TeX
        out.write(flickr_boulat.get_frontmatter_tex(datetime.now()))

        # Consider each photoset
        for photo_set in self.photosets:

            # Write photo set title TeX
            out.write(flickr_boulat.get_photo_set_title_tex(uuid.uuid4(), photo_set))

            # Consider each photo
            photos = photo_set['photos']
            iPh = -1
            nPh = len(photos)
            while iPh < nPh - 1:
                cur_uuid = uuid.uuid4()

                # Write photo layout set up TeX
                out.write(flickr_boulat.get_photo_layout_set_up_tex(cur_uuid))
                
                # Draw a random photo layout
                random.seed()
                draw = random.random()
                nCDF = len(flickr_boulat.contents_photo_layout_cdf)
                for iCDF in range(nCDF):
                    if draw < flickr_boulat.contents_photo_layout_cdf[iCDF]:
                        break
                nRow = flickr_boulat.contents_photo_layout[iCDF][0]
                nCol = flickr_boulat.contents_photo_layout[iCDF][1]

                # Attempt to fill photo layout
                for iRow in range(nRow):
                    if iPh == nPh:
                        break
                    for iCol in range(nCol):
                        iPh += 1
                        if iPh == nPh:
                            break
                        photo = photos[iPh]
                        if "file_name" in photo:
                            out.write(flickr_boulat.get_contents_photo_tex(
                                    photo, iRow, nRow, iCol, nCol))

                # Write photo layout tear down TeX
                out.write(flickr_boulat.get_photo_layout_tear_down_tex(cur_uuid))

        # Write empty pages TeX
        for page in range(empty_pages):
            out.write(flickr_boulat.get_contents_empty_page_tex())

        # Write back matter TeX
        out.write(flickr_boulat.get_backmatter_tex())

        # Write contents tear down TeX
        out.write(flickr_boulat.get_contents_tear_down_tex())
        out.close()

    def write_boulat_cover(self, cover_size, file_name):
        """Write ConTeXt file to produce (Alexandra) Boulat cover.

        """
        # Initialize FlickrBoulat
        flickr_boulat = FlickrBoulat(self.profile_image_file_name, self.background_image_file_name)

        # Write cover set up TeX
        out = codecs.open(file_name, mode='w', encoding='utf-8', errors='ignore')
        out.write(flickr_boulat.get_cover_set_up_tex(cover_size))

        # Collect and shuffle all photos from all photosets
        photos = []
        for photo_set in self.photosets:
            for photo in photo_set['photos']:
                photos.append(photo)
        random.seed()
        random.shuffle(photos)

        # Compute the number of rows and columns
        nPh = len(photos)
        nRow = int(math.floor(math.sqrt(nPh)))
        nCol = nRow
        
        # Consider each photo
        for iPh in range(nRow * nCol):
            photo = photos[iPh]
            iCol = iPh % nCol
            iRow = iPh / nCol
            if "file_name" in photo:
                out.write(flickr_boulat.get_cover_photo_tex(photo, iRow, nRow, iCol, nCol))

        # Write cover tear down TeX
        out.write(flickr_boulat.get_cover_tear_down_tex(
            self.blu_pen_utility.escape_special_characters(self.username), self.created_dt))
        out.close()
