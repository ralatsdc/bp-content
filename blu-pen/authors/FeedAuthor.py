# -*- coding: utf-8 -*-

# Standard library imports
from StringIO import StringIO
import codecs
from datetime import datetime, date, timedelta
import logging
import os
import pickle
from urlparse import urlparse

# Third-party imports
import feedparser
from lxml import etree

# Local imports
from BluePeninsulaUtility import BluePeninsulaUtility
from FeedSmallWhite import FeedSmallWhite

class FeedAuthor:
    """Represents authors of Feeds by their creative output. Authors
    are selected by URL. Books are named after butterflies.

    """
    def __init__(self, blu_pen, source_url, content_dir, requested_dt=datetime.utcnow()):
        """Constructs a FeedAuthor instance given source words.

        """
        self.blu_pen = blu_pen
        self.blu_pen_utility = BluePeninsulaUtility()
        self.source_url = source_url
        self.source_netloc = urlparse(self.source_url).netloc

        self.content_dir = content_dir
        self.pickle_file_name = os.path.join(self.content_dir, self.source_netloc + ".pkl")

        self.requested_dt = requested_dt

        self.content = None
        self.content_set = False

        self.authors = []
        self.start_dt = None
        self.stop_dt = None
        self.tags = set()

        self.cover_rgb = [0.5, 0.5, 0.5]
        
        self.logger = logging.getLogger("blu-pen.FeedAuthor")

    def set_content_as_recent(self):
        """Gets feed content from source URL.

        """
        # Get feed content
        try:
            self.content = feedparser.parse(self.source_url)
        except Exception as exc:
            self.logger.info("{0} could not parse feed content from {1}".format(self.source_netloc, self.source_url))
            raise Exception("Problem with feed")
        self.content_set = True

        # Determine authors
        self.authors = []
        for author in self.content['feed']['authors']:
            self.authors.append(author['name'])
            
        # Determine start and stop datetime, and unique tags
        self.start_dt = datetime(2500, 1, 1)
        self.stop_dt = datetime(1500, 1, 1)
        for entry in self.content['entries']:
            root = etree.fromstring(entry['content'][0]['value'])
            for element in root.iter():

                # Determine start and stop time
                updated_dt = datetime.strptime(entry['updated'][0:19], '%Y-%m-%dT%H:%M:%S')
                if updated_dt < self.start_dt:
                    self.start_dt = updated_dt
                if updated_dt > self.stop_dt:
                    self.stop_dt = updated_dt

                # Determine unique tags
                if not element.tag in self.tags and not element.tag == etree.Comment:
                    self.tags.add(element.tag)
        
    def download_images(self):
        """Download all images by this author from the feed.

        """
        for entry in self.content['entries']:
            for link in entry['links']:
                if link['type'].find('image/') != -1:
                    image_url = link['href']
                    head, tail = os.path.split(image_url)
                    image_file_name = os.path.join(self.content_dir, tail)
                    self.blu_pen_utility.download_file(image_url, image_file_name)
                    self.logger.info("{0} downloaded image to file {1}".format(
                        self.source_netloc, image_file_name))

    def dump(self, pickle_file_name=None):
        """Dumps FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['source_url'] = self.source_url
        p['source_netloc'] = self.source_netloc

        p['content_dir'] = self.content_dir
        p['pickle_file_name'] = self.pickle_file_name

        p['requested_dt'] = self.requested_dt

        p['content'] = self.content
        p['content_set'] = self.content_set

        p['authors'] = self.authors
        p['start_dt'] = self.start_dt
        p['stop_dt'] = self.stop_dt
        p['tags'] = self.tags

        pickle.dump(p, pickle_file)

        self.logger.info("{0} dumped content to {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Loads FeedAuthor attributes pickle.

        """
        if pickle_file_name is None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        p = pickle.load(pickle_file)

        self.source_url = p['source_url']
        self.source_netloc = p['source_netloc']

        self.content_dir = p['content_dir']
        self.pickle_file_name = p['pickle_file_name']

        self.requested_dt = p['requested_dt']

        self.content = p['content']
        self.content_set = p['content_set']

        self.authors = p['authors']
        self.start_dt = p['start_dt']
        self.stop_dt = p['stop_dt']
        self.tags = p['tags']

        self.logger.info("{0} loaded content from {1}".format(self.source_netloc, pickle_file_name))

        pickle_file.close()

    def write_smallwhite_contents(self, book_title, file_name, register={}, start_page=0,
                                  empty_pages=int(0), min_frequency=int(0)):
        """Writes context file to produce SmallWhite contents.
        
        """
        # Initialize FeedSmallWhite
        feed_smallwhite = FeedSmallWhite(self)

        # Write contents set up TeX
        out = codecs.open(file_name, mode='w', encoding='utf-8', errors='ignore')
        self.logger.info("{0} requested datetime is {1}".format(self.source_netloc, self.requested_dt))
        out.write(feed_smallwhite.get_book_set_up_tex(book_title, self.requested_dt))

        # Write front matter TeX
        out.write(feed_smallwhite.get_frontmatter_tex())

        # Write index TeX
        do_write_register = False
        if len(register) > 0:
            do_write_register = True

            # Write index set up TeX
            out.write(feed_smallwhite.get_index_set_up_tex())
        
            # Write index entry TeX
            def index(word):
                if word[0] == "@":
                    return "1" + word
                elif word.find(r"\\letterhash") == 0:
                    return "2" + word
                elif word[0].upper() == "A":
                    return "A" + word
                elif word[0].upper() == "B":
                    return "B" + word
                elif word[0].upper() == "C":
                    return "C" + word
                elif word[0].upper() == "D":
                    return "D" + word
                elif word[0].upper() == "E":
                    return "E" + word
                elif word[0].upper() == "F":
                    return "F" + word
                elif word[0].upper() == "G":
                    return "G" + word
                elif word[0].upper() == "H":
                    return "H" + word
                elif word[0].upper() == "I":
                    return "I" + word
                elif word[0].upper() == "J":
                    return "J" + word
                elif word[0].upper() == "K":
                    return "K" + word
                elif word[0].upper() == "L":
                    return "L" + word
                elif word[0].upper() == "M":
                    return "M" + word
                elif word[0].upper() == "N":
                    return "N" + word
                elif word[0].upper() == "O":
                    return "O" + word
                elif word[0].upper() == "P":
                    return "P" + word
                elif word[0].upper() == "Q":
                    return "Q" + word
                elif word[0].upper() == "R":
                    return "R" + word
                elif word[0].upper() == "S":
                    return "S" + word
                elif word[0].upper() == "T":
                    return "T" + word
                elif word[0].upper() == "U":
                    return "U" + word
                elif word[0].upper() == "V":
                    return "V" + word
                elif word[0].upper() == "W":
                    return "W" + word
                elif word[0].upper() == "X":
                    return "X" + word
                elif word[0].upper() == "Y":
                    return "Y" + word
                elif word[0].upper() == "Z":
                    return "Z" + word
            first = True
            label = ""
            for word in sorted(register.keys(), key=index):
                if (word in self.frequency
                    and self.frequency[word] < min_frequency):
                    continue
                clean_word = self.blu_pen_utility.escape_special_characters(word, for_use_in="index")
                if clean_word[0:11].find(r"\letterhash") != -1:
                    clean_label = r"\letterhash"
                else:
                    clean_label = clean_word[0]
                clean_label = clean_label.lower()
                # TODO: Find a way to replace quotes here.
                # clean_word = self.blu_pen_utility.replace_quotation_marks(word).replace(r"\ ", r"\\")
                real_pages = sorted(register[word])
                user_pages = [p - start_page + 1 for p in real_pages]
                clean_pages = str(user_pages).replace("[", "").replace("]", "")
                if clean_label != label:
                    label = clean_label
                    if first:
                        first = False
                        out.write(feed_smallwhite.get_index_section_tex("", "neither"))
                    else:
                        out.write(feed_smallwhite.get_index_section_tex("", "both"))
                out.write(feed_smallwhite.get_index_entry_tex(clean_word, clean_pages))

            # Write index tear down TeX
            out.write(feed_smallwhite.get_index_tear_down_tex())

        # Write contents set up TeX
        out.write(feed_smallwhite.get_contents_set_up_tex())

        # Write text for each entry in chapters
        for entry in self.content['entries']:
            
            # Write contents title TeX
            out.write(feed_smallwhite.get_contents_title_tex(entry['title']))

            # Write contents image TeX
            for link in entry['links']:
                if link['type'].find('image/') != -1:
                    image_url = link['href']
                    head, tail = os.path.split(image_url)
                    image_file_name = os.path.join(self.content_dir, tail)
                    out.write(feed_smallwhite.get_contents_image_tex(image_file_name))

            # Parse the article (root) element
            article = etree.fromstring(entry['content'][0]['value'])

            # Find each section element
            for section in article.iterfind('section'):

                # Find each subelement in the current section element
                for element in section.iterchildren():

                    # Write tag TeX
                    if element.tag == 'blockquote':
                        text = self.get_element_text(element)
                        out.write(feed_smallwhite.get_contents_blockquote_tag_tex(text))
                    
                    elif element.tag == 'h3':
                        text = self.get_element_text(element)
                        out.write(feed_smallwhite.get_contents_h3_tag_tex(text))

                    elif element.tag == 'h4':
                        text = self.get_element_text(element)
                        out.write(feed_smallwhite.get_contents_h4_tag_tex(text))

                    elif element.tag == 'p':
                        subelement = element.find('cite')
                        if subelement != None:
                            cite = self.get_element_text(subelement)
                            text = self.get_element_text(subelement, part='tail')
                            out.write(feed_smallwhite.get_contents_cite_tag_tex(cite))
                            out.write(feed_smallwhite.get_contents_p_tag_tex(text))
                        else:
                            text = self.get_element_text(element)
                            out.write(feed_smallwhite.get_contents_p_tag_tex(text))
                
                    elif element.tag == 'ul':
                        text = self.get_element_text(element)
                        out.write(feed_smallwhite.get_contents_ul_tag_tex(text, "start"))
                        for subelement in element.iterchildren():
                            anchor = subelement.find('a')
                            # link = ???
                            text = self.get_element_text(anchor)
                            out.write(feed_smallwhite.get_contents_li_tag_tex(text))
                        out.write(feed_smallwhite.get_contents_ul_tag_tex(None, "stop"))

                    elif element.tag == 'em':
                        text = self.get_element_text(element)
                        out.write(feed_smallwhite.get_contents_em_tag_tex(text))

        # Write contents tear down TeX
        out.write(feed_smallwhite.get_contents_tear_down_tex())

        # Write empty pages
        for page in range(empty_pages):
            out.write(feed_smallwhite.get_contents_empty_page_tex())
            
        # Write back matter TeX
        out.write(feed_smallwhite.get_backmatter_tex())

        # Write contents tear down TeX
        out.write(feed_smallwhite.get_book_tear_down_tex())
        out.close()

    def write_smallwhite_cover(self, book_title, cover_size, file_name):
        """Writes context file to produce SmallWhite cover.
        
        """
        # Initialize FeedSmallWhite
        feed_smallwhite = FeedSmallWhite(self)

        # Write set up TeX
        out = codecs.open(file_name, mode='w', encoding='utf-8', errors='ignore')
        out.write(feed_smallwhite.get_cover_set_up_tex(cover_size, self.cover_rgb))

        # Write tear down TeX
        self.logger.info("{0} requested datetime is {1}".format(self.source_netloc, self.requested_dt))
        out.write(feed_smallwhite.get_cover_tear_down_tex(
            book_title, self.requested_dt, self.cover_rgb))
        out.close()

    def get_element_text(self, element, part='text'):
        """Prepares element text for typsetting.

        """
        if part == 'text':
            text = element.text
        elif part == 'tail':
            text = element.tail
        else:
            raise Exception("Unknown part: {0}".format(part))
        if text != None:
            text = text.replace('\n', '')
            if len(text) != 0:
                text = text.encode('utf_8', 'ignore')
            else:
                text = None

        return text
