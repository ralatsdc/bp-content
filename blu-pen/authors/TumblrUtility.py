#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
from datetime import datetime, timedelta
import logging
import os
import re
import urllib
import uuid

# Third-party imports
from BeautifulSoup import BeautifulSoup
from lxml.html import soupparser

# Local imports
from BluePeninsula import BluePeninsula
from BluePeninsulaUtility import BluePeninsulaUtility
from TumblrAuthor import TumblrAuthor
from TumblrWheat import TumblrWheat

class TumblrUtility:
    """Represents utilities for using Tumblr.

    """
    def __init__(self):
        """Constructs a TumblrUtility.

        """
        self.blu_pen = BluePeninsula("BluePeninsula.cfg")
        self.blu_pen_utility = BluePeninsulaUtility()
        self.logger = logging.getLogger(__name__)

    def get_popular_tags(self):
        """Get popular tags from the explore page.

        """
        # Read the explore page
        try:
            html = urllib.urlopen("http://www.tumblr.com/explore").read()
        except Exception as exc:
            self.logger.error("could not get popular tags")
        
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
            self.logger.error("could not get subdomains for tag")
        
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
        content_dir = os.path.join(self.blu_pen.tumblr_content_dir, subdomain)
        if not os.path.exists(content_dir):
            os.makedirs(content_dir)

        # Get posts for the Tumblr author

        tumblr_author.set_posts_as_recent()

        # Count post types for the Tumblr author
        regular = 0
        word = 0
        photo = 0
        for post in tumblr_author.posts:
            if post['type'] == "regular":
                regular += 1
                if "regular-body" in post and post['regular-body'] != None:
                    word += len(self.blu_pen_utility.strip_html(post['regular-body']).split())
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

    def write_wheat_photo_grid_layouts(self):
        """Write wheat layouts.

        """
        # Initialize a TumblrAuthor and TumblrWheat
        subdomain = "thesoundarabbitmakes"
        content_dir = os.path.join(self.blu_pen.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self.blu_pen, subdomain, content_dir)
        tumblr_wheat = TumblrWheat(tumblr_author)
        tumblr_wheat.marking = "on"
        
        # Get contents set up TeX
        book_title = "Ah...This is the Life"
        created_dt = [datetime.now()]
        requested_dt = datetime.now()
        tex = tumblr_wheat.get_contents_set_up_tex(book_title, created_dt, requested_dt)

        # Consider each photo grid
        n_grids = len(tumblr_wheat.photo_grids)
        for i_grid in range(n_grids):
            cur_uuid = uuid.uuid4()

            # Write photo layout set up TeX
            tex += """
\startuniqueMPgraphic{{{uuid}}}
  page_width := {contents_page_width}in;
  page_height := {contents_page_height}in;

  draw (0, 0)--(page_width, page_height)
    withpen pencircle scaled 1bp withcolor white;
""".format(uuid=cur_uuid,
           contents_page_width=tumblr_wheat.contents_page_width,
           contents_page_height=tumblr_wheat.contents_page_height)
                
            # Reset the photo grid's photo group index
            tumblr_wheat.photo_grids[i_grid].reset_photo_group_index()

            # Draw each photo group of the photo grid
            i_group = tumblr_wheat.photo_grids[i_grid].get_next_photo_group_in_order()
            while i_group != None:
                # Compute the photo group width and height
                photo_grp_coords = tumblr_wheat.photo_grids[i_grid].right.get_photo_group_coordinates(i_group)
                photo_grp_width = photo_grp_coords['x_r'] - photo_grp_coords['x_l']
                photo_grp_height = photo_grp_coords['y_t'] - photo_grp_coords['y_b']
                 
                # Compute the photo group x and y shift
                photo_grp_x_shift = tumblr_wheat.photo_grid_x_shift + photo_grp_coords['x_l']
                photo_grp_y_shift = tumblr_wheat.photo_grid_y_shift + photo_grp_coords['y_b']

                tex += """
  photo_grp_x_shift := {photo_grp_x_shift}in;
  photo_grp_y_shift := {photo_grp_y_shift}in;
  photo_grp_width := {photo_grp_width}in;
  photo_grp_height := {photo_grp_height}in;

  draw (photo_grp_x_shift, photo_grp_y_shift)
    --(photo_grp_x_shift, photo_grp_y_shift + photo_grp_height)
    --(photo_grp_x_shift + photo_grp_width, photo_grp_y_shift + photo_grp_height)
    --(photo_grp_x_shift + photo_grp_width, photo_grp_y_shift)
    --(photo_grp_x_shift, photo_grp_y_shift)
    withpen pencircle scaled 2bp withcolor red;

  picture labeltext;
  labeltext := btex {{ {i_group} }} etex;
  labeloffset := 0bp;
  label.urt(labeltext, (photo_grp_x_shift, photo_grp_y_shift));
""".format(photo_grp_width=photo_grp_width,
           photo_grp_height=photo_grp_height,
           photo_grp_x_shift=photo_grp_x_shift,
           photo_grp_y_shift=photo_grp_y_shift,
           i_group=i_group)

                tex += ""
                # Get the next photo group
                i_group = tumblr_wheat.photo_grids[i_grid].get_next_photo_group_in_order()

            # Compute the text group width and height
            text_grp_coords = tumblr_wheat.photo_grids[i_grid].get_text_group_coordinates()
            text_grp_width = text_grp_coords['x_r'] - text_grp_coords['x_l']
            text_grp_height = text_grp_coords['y_t'] - text_grp_coords['y_b']
                            
            # Compute the text group x and y shift
            text_grp_x_shift = tumblr_wheat.photo_grid_x_shift + text_grp_coords['x_l']
            text_grp_y_shift = tumblr_wheat.photo_grid_y_shift + text_grp_coords['y_b']

            tex += """
  text_grp_x_shift := {text_grp_x_shift}in;
  text_grp_y_shift := {text_grp_y_shift}in;
  text_grp_width := {text_grp_width}in;
  text_grp_height := {text_grp_height}in;

  draw (text_grp_x_shift, text_grp_y_shift)
    --(text_grp_x_shift, text_grp_y_shift + text_grp_height)
    --(text_grp_x_shift + text_grp_width, text_grp_y_shift + text_grp_height)
    --(text_grp_x_shift + text_grp_width, text_grp_y_shift)
    --(text_grp_x_shift, text_grp_y_shift)
    withpen pencircle scaled 2bp withcolor blue;

  picture labeltext;
  labeltext := btex {{ text }} etex;
  labeloffset := 0bp;
  label.urt(labeltext, (text_grp_x_shift, text_grp_y_shift));
""".format(text_grp_width=text_grp_width,
           text_grp_height=text_grp_height,
           text_grp_x_shift=text_grp_x_shift,
           text_grp_y_shift=text_grp_y_shift)

            # Write photo layout tear down TeX
            i_layout = tumblr_wheat.photo_grids[i_grid].right.i_layout
            tex += """
\stopuniqueMPgraphic

\defineoverlay[{uuid}][\uniqueMPgraphic{{{uuid}}}]

\page
\setupbackgrounds[page][background={{{uuid}}}]
\setuplayout[contents_{i_layout}]
\midaligned{{\hskip1em}}
""".format(uuid=cur_uuid,
           i_layout=i_layout)

        tex += """
\page
\setupbackgrounds[page][background={{{{}}}}]
\setuplayout[contents_{i_layout}]
\midaligned{{\hskip1em}}
\stoptext
""".format(i_layout=tumblr_wheat.contents_default_i_layout)
        
        # Write TeX to a file
        out = open(subdomain + ".tex", 'w')
        out.write(tex)
        out.close()


    def write_posts_from_thesoundarabbitmakes(self):
        """Write posts from thesoundarabbitmakes.

        """
        # Initialize a TumblrAuthor and TumblrWheat
        subdomain = "thesoundarabbitmakes"
        content_dir = os.path.join(self.blu_pen.tumblr_content_dir, subdomain)
        tumblr_author = TumblrAuthor(self.blu_pen, subdomain, content_dir)
        tumblr_wheat = TumblrWheat(tumblr_author)

        # Load posts
        tumblr_author.load()

        # Write each post as received content and processed text
        for post in tumblr_author.posts:
            if post['type'] == "regular":

                # Prepare post content for typesetting
                if "regular-title" in post and  post['regular-title'] != None:
                    print "\npost['regular-title']:\n", post['regular-title']
                    regular_title = self.tumblr_wheat.get_post_text(post['regular-title'])
                    print "\nregular_title:\n", regular_title
                else:
                    regular_title = ""
                if "regular-body" in post and post['regular-body'] != None:
                    print "\npost['regular-body']:\n", post['regular-body']
                    regular_body = self.tumblr_wheat.get_post_text(post['regular-body'])
                    print "\nregular_body:\n", regular_body
                else:
                    regular_body = ""

            elif post['type'] == "link":

                # Prepare post content for typesetting
                if "link-text" in post and post['link-text'] != None:
                    print "\npost['link-text']:\n", post['link-text']
                    link_text = self.tumblr_wheat.get_post_text(post['link-text'])
                    print "\nlink_text:\n", link_text
                else:
                    link_text = ""
                if "link-description" in post and post['link-description'] != None:
                    print "\npost['link-description']:\n", post['link-description']
                    link_description = self.tumblr_wheat.get_post_text(post['link-description'])
                    print "\nlink_description:\n", link_description
                else:
                    link_description = ""
                if "link-url" in post and post['link-url'] != None:
                    print "\npost['link-url']:\n", post['link-url']
                    link_url = self.tumblr_wheat.get_post_text(post['link-url'])
                    print "\nlink_url:\n", link_url
                else:
                    link_url = ""

            elif post['type'] == "quote":

                # Prepare post content for typesetting
                if "quote-source" in post and post['quote-source'] != None:
                    print "\npost['quote-source']:\n", post['quote-source']
                    quote_source = self.tumblr_wheat.get_post_text(post['quote-source'])
                    print "\nquote_source:\n", quote_source
                else:
                    quote_source = ""
                if "quote-text" in post and post['quote-text'] != None:
                    print "\npost['quote-text']:\n", post['quote-text']
                    quote_text = self.tumblr_wheat.get_post_text(post['quote-text'])
                    print "\nquote_text:\n", quote_text
                else:
                    quote_text = ""
                
            elif post['type'] == "photo":

                if "photo-caption" in post and post['photo-caption'] != None:
                    print "\npost['photo-caption']:\n", post['photo-caption']
                    photo_caption = self.tumblr_wheat.get_post_text(post['photo-caption'])
                    print "\nphoto_caption:\n", photo_caption
                else:
                    photo_caption = ""

if __name__ == "__main__":

    tumblr_utility = TumblrUtility()

    tumblr_utility.write_wheat_photo_grid_layouts()

    # tumblr_utility.write_posts_from_thesoundarabbitmakes()
