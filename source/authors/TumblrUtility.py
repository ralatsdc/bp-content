#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
import logging
import os
import re
import urllib

# Third-party imports
from BeautifulSoup import BeautifulSoup

# Local imports
from authors.BluPenAuthor import BluPenAuthor
from authors.TumblrAuthor import TumblrAuthor
from utility.AuthorsUtility import AuthorsUtility

class TumblrUtility:
    """Represents utilities for using Tumblr.

    """
    def __init__(self):
        """Constructs a TumblrUtility.

        """
        self.blu_pen_author = BluPenAuthor("BluPenAuthor.cfg")
        self.authors_utility = AuthorsUtility()
        self.logger = logging.getLogger(__name__)

    def get_popular_tags(self):
        """Get popular tags from the explore page.

        """
        # Read the explore page
        try:
            html = urllib.urlopen("http://www.tumblr.com/explore").read()
        except Exception as exc:
            self.logger.error(u"Could not get popular tags")
        
        # Find all anchors in the page containing tags
        p = re.compile("^/tagged/(.*)$")
        tags = set()
        soup = BeautifulSoup(html)
        for anchor in soup.findAll("a"):
            url = anchor['href']
            m = p.match(url)
            if m:
                # Collect unique tags
                tag = m.group(1)
                tags.add(tag)

        return list(tags)

    def get_subdomains_for_tag(self, tag):
        """Get subdomains for the tag.

        """
        # Read the page for the tag
        try:
            html = urllib.urlopen("http://www.tumblr.com/tagged/" + tag).read()
        except Exception as exc:
            self.logger.error(u"Could not get subdomains for tag")
        
        # Find all links in the page containing subdomains
        p = re.compile("^http://(.*).tumblr.com$")
        subdomains = set()
        soup = BeautifulSoup(html)
        for anchor in soup.findAll("a"):
            url = anchor['href']
            m = p.match(url)
            if m:
                # Collect unique subdomains
                subdomain = m.group(1)
                subdomains.add(subdomain)

        return list(subdomains)

    def count_posts_for_subdomain(self, subdomain):
        """Count post types for the subdomain.

        """
        # Create the content directory for the Tumblr author, if needed
        content_dir = os.path.join(self.blu_pen_author.tumblr_content_dir, subdomain)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Get posts for the Tumblr author
        tumblr_author = TumblrAuthor(self.blu_pen_author, subdomain, content_dir)
        tumblr_author.set_posts()

        # Count post types for the Tumblr author
        regular = 0
        word = 0
        photo = 0
        for post in tumblr_author.posts:
            if post['type'] == "regular":
                regular += 1
                if "regular-body" in post and post['regular-body'] != None:
                    word += len(self.authors_utility.strip_html(post['regular-body']).split())
            elif post['type'] == "photo":
                photo += 1

        return {"total": len(tumblr_author.posts), "regular": regular, "word": word, "photo": photo}

    def count_posts_for_subdomains(self, subdomains, words_per_page=500.0):
        """Count and print post types for the subdomains.

        """
        print "{0:^30s} {1:^10s} {2:^10s} {3:^10s} {4:^10s} {5:^20s} {6:^20s} {7:^20s}".format(
            "subdomain", "regular", "word", "photo", "total", "regular/photo", "word/photo", "page/photo")
        for subdomain in subdomains:
            count = self.count_posts_for_subdomain(subdomain)
            if count['photo'] != 0:
                print "{0:^30s} {1:^10d} {2:^10d} {3:^10d} {4:^10d} {5:^20.3f} {6:^20.3f} {7:^20.3f}".format(
                    subdomain, count['regular'], count['word'], count['photo'], count['total'],
                    1.0 * count['regular'] / count['photo'],
                    count['word'] / count['photo'],
                    count['word'] / words_per_page / count['photo'])
            else:
                print "{0:^30s} {1:^10d} {2:^10d} {3:^10d} {4:^10d} {5:^20s} {6:^20s} {7:^20s}".format(
                    subdomain, count['regular'], count['word'], count['photo'], count['total'],
                    "---", "---", "---")
