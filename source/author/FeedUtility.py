# -*- coding: utf-8 -*-

# Standard library imports
import logging

# Third-party imports

# Local imports

class FeedUtility(object):
    """Represents utilities for using feeds.

    """

    def __init__(self):
        """Constructs a FeedUtility.

        """
        self.logger = logging.getLogger(__name__)
        
    def get_content(self, entry):
        """
        Returns content value of entry using a heirarchy of keys.

        """
        if entry['content'] != None:
            return entry['content'][0]
        elif entry['summary_detail'] != None:
            return entry['summary_detail']
        else:
            return None
