# -*- coding: utf-8 -*-

# Standard library imports
import logging
import xml

# Third-party imports
import flickrapi

# Local imports

class FlickrUtility:
    """Represents utilities for using Flickr.

    """
    def __init__(self,
                 api_key = '71ae5bd2b331d44649161f6d3ff7e6b6', api_secret = '45f1be4bd59f9155'):
        """Constructs a FlickrUtility.

        """
        self.api_key = api_key
        self.api_secret = api_secret

        self.api = flickrapi.FlickrAPI(self.api_key)
        
        self.logger = logging.getLogger(__name__)
        
    def get_names_from_term(self, term):
        """Gets usernames for photos returned by searching for a term
        in the photo's title, description or tags.

        """
        # Get photos by searching for a term
        try:
            photos_xml = self.api.interestingness_getList()
        except Exception as exc:
            self.logger.error("could not get photos XML: {0}".format(
                exc))

        # Parse attributes and values
        usernames = set()
        for photo_xml in photos_xml.find("photos").findall("photo"):
            if len(usernames) > 10:
                break
            user_id = photo_xml.get("owner")

            try:
                info_xml = self.api.people_getInfo(user_id=user_id)
            except Exception as exc:
                self.logger.error("could not get user info XML for {0}: {1}".format(
                    user_id, exc))
            usernames.add(info_xml.find("person").findtext("username"))

        return usernames
