# -*- coding: utf-8 -*-

# Standard library imports
import codecs
import collections
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import random
import re
import smtplib
from time import sleep
import urllib, urllib2

# Third-party imports

# Local imports
from ServiceError import ServiceError

class BluePeninsulaUtility:
    """Provides utilities for using an BluePeninsula.

    """
    def __init__(self, number_of_download_attempts=3,
                 seconds_between_download_attempts=3,
                 max_character_repetitions=6):
        """Constructs an BluePeninsulaUtility.

        """
        self.number_of_download_attempts = number_of_download_attempts
        self.seconds_between_download_attempts = float(seconds_between_download_attempts)
        self.max_character_repetitions = int(max_character_repetitions);

        self.logger = logging.getLogger(__name__)

    def download_file(self, req_file_url, out_file_name):
        """Downloads a file from a URL.

        """
        # Cowardly refuse to download the URL to an unnamed file
        if out_file_name == "":
            raise ServiceError("Cowardly refusing to download the URL to an unnamed file.")

        # Download the URL to a response file in memory
        do_download = True
        iAttempts = 0
        while do_download and iAttempts < self.number_of_download_attempts:
            iAttempts += 1
            try:
                res_file = urllib2.urlopen(req_file_url)
                do_download = False
            except Exception as exc:
                sleep(self.seconds_between_download_attempts)
        if do_download:
            raise ServiceError("Couldn't download URL {0}: {0}".format(req_file_url, exc))

        # Write the response file to the output file
        out_file = open(out_file_name, 'w')
        out_file.write(res_file.read())
        out_file.close()

    def send_mail_text(self, to_address, from_address, from_password, subject, text_file):
        """Sends an email message from a plain text file.

        """
        # Create a text/plain message from a file.
        if os.path.exists(text_file):
            fp = codecs.open(text_file, mode='r', encoding='ascii', errors='ignore')
            msg = MIMEText(fp.read())
            fp.close()
        else:
            msg = MIMEText(text_file)
        msg['To'] = to_address
        msg['From'] = from_address
        msg['Subject'] = subject

        # Send the message via GMail.
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(from_address, from_password)
        smtp.sendmail(from_address, [to_address], msg.as_string())
        smtp.close()

    def send_mail_html(self, to_address, from_address, from_password,
                       subject, text_file, html_file):
        """Sends an HTML email message with a plain text alternative
        from an HTML and plain text file.

        """
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['To'] = to_address
        msg['From'] = from_address
        msg['Subject'] = subject

        # Record the MIME types of both parts - text/plain and text/html.
        fp = codecs.open(text_file, mode='r', encoding='ascii', errors='ignore')
        part1 = MIMEText(fp.read(), 'plain')
        fp.close()
        fp = codecs.open(html_file, mode='r', encoding='ascii', errors='ignore')
        part2 = MIMEText(fp.read(), 'html')
        fp.close()

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message,
        # in this case the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)

        # Send the message via GMail.
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(from_address, from_password)
        smtp.sendmail(from_address, [to_address], msg.as_string())
        smtp.close()
  
    def strip_html(self, text):
        """Removes HTML markup from a text string.

        Params:
        ... text -- The HTML source.

        Return: The plain text.

        If the HTML source contains non-ASCII entities or character
        references, this is a Unicode string.

        Author: Fredrik Lundh
        Version: January 15, 2003
        See: http://effbot.org/zone/re-sub.htm#unescape-html

        """
        def fixup(m):
            text = m.group(0)
            if text[:1] == "<":
                return "" # ignore tags
            if text[:2] == "&#":
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            elif text[:1] == "&":
                import htmlentitydefs
                entity = htmlentitydefs.entitydefs.get(text[1:-1])
                if entity:
                    if entity[:2] == "&#":
                        try:
                            return unichr(int(entity[2:-1]))
                        except ValueError:
                            pass
                    else:
                        return unicode(entity, "iso-8859-1")
            return text # leave as is
        return re.sub("(?s)<[^>]*>|&#?\w+;", fixup, text)

    def escape_special_characters(self, text, link_style=None, for_use_in=""):
        """Escapes TeX special characters, and styles and hyphenates URLs.

        """
        # Escape TeX special characters
        if for_use_in == "metapost":
            text = text.replace("\\", r"\letterbackslash letterbackslash ") # Do not use r"\\"
            text = text.replace(r"~", r"\letterbackslash lettertilde ")
            text = text.replace(r"#", r"\letterbackslash letterhash ")
            text = text.replace(r"$", r"\letterbackslash letterdollar ")
            text = text.replace(r"%", r"\letterbackslash letterpercent ")
            text = text.replace(r"^", r"\letterbackslash letterhat ")
            text = text.replace(r"&", r"\letterbackslash letterampersand ")
            text = text.replace(r"{", r"\letterbackslash letteropenbrace ")
            text = text.replace(r"}", r"\letterbackslash letterclosebrace ")
            text = text.replace(r"|", r"\letterbackslash letterbar ")
            if text.find(r"http://") == -1 and text.find(r"www.") == -1:
                text = text.replace(r"_", r"\letterbackslash letterunderscore ")
        else:
            text = text.replace("\\", r"\letterbackslash ") # Do not use r"\\"
            text = text.replace(r"~", r"\lettertilde ")
            text = text.replace(r"#", r"\letterhash ")
            text = text.replace(r"$", r"\letterdollar ")
            text = text.replace(r"%", r"\letterpercent ")
            text = text.replace(r"^", r"\letterhat ")
            text = text.replace(r"&", r"\letterampersand ")
            text = text.replace(r"{", r"\letteropenbrace ")
            text = text.replace(r"}", r"\letterclosebrace ")
            text = text.replace(r"|", r"\letterbar ")
            if text.find(r"http://") == -1 and text.find(r"www.") == -1:
                text = text.replace(r"_", r"\letterunderscore ")
        if for_use_in == "index":
            text = text.replace(r"\letterbackslash \letterbackslash ", "\\") # Do not use r"\\"
            
        # Apply link style, if needed, and hyphenate URLs
        if text.find(r"http://") != -1 or text.find(r"www.") != -1:
            # text = text.replace(r"_", prefix + r"\letterunderscore ")
            idx = [text.find(r"http://"), text.find(r"www.")]
            try:
                idx.remove(-1)
            except Exception as exc:
                pass
            idx = min(idx)
            head = self.escape_special_characters(text[0:idx])
            if link_style == None:
                text = head + r"\hyphenatedurl{" + text[idx:] + r"}"
            else:
                text = head + r"{" + link_style + r"\hyphenatedurl{" + text[idx:] + r"}}"

        return text

    def remove_punctuation_marks(self, word):
        """Removes leading and trailing punctuation marks from a word
        split from a line.

        """
        word = re.sub(r'^[.\':,\-!.?";/]+', "", word)
        word = re.sub(r'[.\':,\-!.?";/]+$', "", word)
        return word

    def replace_quotation_marks(self, text):
        """Replaces quotation marks with ConTeXt commands.

        """
        # Replace single quotes
        text = text.replace(r" '", r" \upperleftsinglesixquote ")
        text = text.replace(r"}'", r"}\upperleftsinglesixquote ")
        text = text.replace(r"'", r"\upperrightsingleninequote ")

        # Replace double quotes
        text = text.replace(r' "', r" \upperleftdoublesixquote ")
        text = text.replace(r'}"', r"}\upperleftdoublesixquote ")
        text = text.replace(r'"', r"\upperrightdoubleninequote ")
        return text

    def add_clean_characters(self, inp_word, add_char):
        """Adds a character given possible escaping of special
        characters.

        """
        if (inp_word.find(r"{\textunderscore}") != -1 or inp_word.find(r"\type{^}") != -1):
            words_a = inp_word.split(r"{\textunderscore}")
            out_word = ""
            for word_a in words_a:
                if word_a.find(r"\type{^}") != -1:
                    words_b = word_a.split(r"\type{^}")
                    word_c = ""
                    for word_b in words_b:
                        if word_c == "":
                            word_c = add_characters(word_b, add_char)
                        else:
                            word_c += r"\type{^}" + add_characters(word_b, add_char)
                else:
                    word_c = add_characters(word_a, add_char)
                if out_word == "":
                    out_word = word_c
                else:
                    out_word += r"{\textunderscore}" + word_c
        else:
            out_word = add_characters(inp_word, add_char)
        return out_word.replace(r"\ ", r"\\")

        def add_characters(inp_word, add_char):
            out_word = ""
            for iChr in range(len(inp_word)):
                if iChr % 6 == 3:
                    out_word += add_char
                out_word += inp_word[iChr]
            return out_word

    def draw_random_flickr_photo(self, flickr_author):
        """Draws a random Blue Peninsula Flickr photo.

        """
        random_file = None
        nAtt = 20
        iAtt = 0
        while random_file == None:
            iAtt += 1
            iPS = random.randint(0, len(flickr_author.photosets) - 1)
            random_photoset = flickr_author.photosets[iPS]
            iPh = random.randint(0, len(random_photoset['photos']) - 1)
            random_photo = random_photoset['photos'][iPh]
            random_photo_url = random_photo['url_m']
            if random_photo_url != None:
                head, tail = os.path.split(random_photo_url)
                random_file = os.path.join(flickr_author.content_dir, tail)
                break
            if iAtt == nAtt:
                break
        return random_file

    def get_hyphenation(self, inp_word):
        """Gets the hyphenaction for repeated characters.

        """
        count = collections.defaultdict(int)
        for char in inp_word:
            count[char] += 1
        hyp_word = inp_word
        for char, rep in count.iteritems():
            if rep > self.max_character_repetitions:
                rep_char = ""
                iCh = 0
                while iCh < self.max_character_repetitions:
                    iCh += 1
                    rep_char += char
                hyp_word = hyp_word.replace(rep_char, rep_char + "-")
        return hyp_word

    def process_source_words(self, source_words_string):
        """Processes source words to identify types, and create log,
        path, header, and label strings. The source words are assumed
        to be contained in a single string and delimited by plus
        signs.

        """
        # Process source words to identify types and words, retaining
        # only two of each, and words only if of the first type
        source_types = []
        source_words = []
        for word in source_words_string.split("+"):
            if len(source_types) == 0:
                if word[0] == "@":
                    # First author explicit
                    source_types.append("@")
                    source_words.append(word[1:])
                elif word[0] == "#":
                    # First hashtag explicit
                    source_types.append("#")
                    source_words.append(word[1:])
                else:
                    # First author assumed
                    source_types.append("@")
                    source_words.append(word)
            else:
                if word[0] == "@" and source_types[0] == "@":
                    # Second author explicit if first author found
                    source_types.append("@")
                    source_words.append(word[1:])
                elif word[0] == "#" and source_types[0] == "#":
                    # Second hashtag explicit if first hashtag found
                    source_types.append("#")
                    source_words.append(word[1:])
                elif word[0] != "#" and source_types[0] == "@":
                    # Second author assumed if first author found
                    source_types.append("@")
                    source_words.append(word)
            if len(source_words) == 2:
                break
        
        # Create log, path, header, label, and url strings
        if len(source_words) == 1:
            if source_types[0] == "@":
                source_log = source_words[0]
                source_path = "by_" + source_words[0]
                source_header = source_words[0]
                source_label = "by " + source_words[0]
            elif source_types[0] == "#":
                source_log = source_words[0]
                source_path = "for_" + source_words[0]
                source_header = source_words[0]
                source_label = "for " + source_words[0]
        else:
            if source_types[0] == "@" and source_types[1] == "@":
                source_log = source_words[0] + " and " + source_words[1]
                source_path = "by_" + source_words[0] + "_and_" + source_words[1]
                source_header = source_words[0] + " and " + source_words[1]
                source_label = "by " + source_words[0] + " and " + source_words[1]
            if source_types[0] == "@" and source_types[1] == "#":
                source_log = source_words[0] + " and " + source_words[1]
                source_path = "by_" + source_words[0] + "_and_for_" + source_words[1]
                source_header = source_words[0] + " and " + source_words[1]
                source_label = "by " + source_words[0] + " and for " + source_words[1]
            if source_types[0] == "#" and source_types[1] == "@":
                source_log = source_words[0] + " and " + source_words[1]
                source_path = "for_" + source_words[0] + "_and_by_" + source_words[1]
                source_header = source_words[0] + " and " + source_words[1]
                source_label = "for " + source_words[0] + " and by " + source_words[1]
            if source_types[0] == "#" and source_types[1] == "#":
                source_log = source_words[0] + " and " + source_words[1]
                source_path = "for_" + source_words[0] + "_and_" + source_words[1]
                source_header = source_words[0] + " and " + source_words[1]
                source_label = "for " + source_words[0] + " and " + source_words[1]

        return (source_log,
                source_path,
                source_header,
                source_label,
                source_types,
                source_words)
