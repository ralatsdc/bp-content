# Finding tags

import codecs
import json
import pprint
import urlparse

import numpy as np
import matplotlib.pyplot as plt

from authors.BluPenAuthor import BluPenAuthor
from authors.FeedAuthor import FeedAuthor
from authors.FlickrGroup import FlickrGroup
from authors.TumblrAuthor import TumblrAuthor
from authors.TwitterAuthor import TwitterAuthor
from utility.AuthorsUtility import AuthorsUtility

config_file = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/blu-pen/authors/BluPenAuthor.cfg"

blu_pen_author = BluPenAuthor(config_file)
authors_utility = AuthorsUtility()

inp_file_names = ["feed.json"]
inp_file_names = ["flickr.json"]
inp_file_names = ["tumblr.json"]
inp_file_names = ["twitter.json"]

inp_file_names = ["feed.json", "flickr.json", "tumblr.json", "twitter.json"]

keys = []
tags = {}

for inp_file_name in inp_file_names:
    pprint.pprint(inp_file_name)

    inp_file = codecs.open(inp_file_name, encoding='utf-8', mode='r')
    inp_data = json.loads(inp_file.read())
    inp_file.close()

    if inp_data['service'] == "feed":

        for author in inp_data['authors']:
            if not author['include']:
                continue

            source_url = author['url']

            content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/feed"

            try:
                feed_author = FeedAuthor(blu_pen_author, source_url, content_dir)
                feed_author.load()
            except Exception as exc:
                print exc

            for entry in feed_author.entries:
                if 'tags' in entry['content'][0]:
                    for tag in entry['content'][0]['tags']:
                        key = tag.encode('utf-8')
                        if not key in keys:
                            keys.append(key)
                            tags[key] = 1
                        else:
                            tags[key] += 1

    if inp_data['service'] == "flickr":

        for group in inp_data['groups']:
            if not group['include']:
                continue
                
            source_word_str = u"@" + group['name']
            group_id = group['nsid']

            content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/flickr"

            try:
                flickr_group = FlickrGroup(blu_pen_author, source_word_str, group_id, content_dir)
                flickr_group.load()
            except Exception as exc:
                next

            for photo in flickr_group.photos:
                if 'tags' in photo:
                    for tag in photo['tags'].split():
                        key = tag.encode('utf-8')
                        if not key in keys:
                            keys.append(key)
                            tags[key] = 1
                        else:
                            tags[key] += 1

    elif inp_data['service'] == "tumblr":

        for author in inp_data['authors']:
            if not author['include']:
                continue

            subdomain = urlparse.urlparse(author['url']).netloc

            content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/tumblr"

            try:
                tumblr_author = TumblrAuthor(blu_pen_author, subdomain, content_dir)
                tumblr_author.load()
            except Exception as exc:
                print exc

            for post in tumblr_author.posts:
                if 'tags' in post:
                    for tag in post['tags']:
                        key = tag.encode('utf-8')
                        if not key in keys:
                            keys.append(key)
                            tags[key] = 1
                        else:
                            tags[key] += 1

    elif inp_data['service'] == "twitter":

        for author in inp_data['authors']:
            if not author['include']:
                continue

            source_words_str = u"@" + author['screen_name']

            content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/twitter"

            try:
                twitter_author = TwitterAuthor(blu_pen_author, source_words_str, content_dir)
                twitter_author.load()
            except Exception as exc:
                next

            for text in twitter_author.clean_text:
                for tag in [token[1:] for token in text.split() if token.startswith('#')]:
                    key = tag # Already unicode
                    if not key in keys:
                        keys.append(key)
                        tags[key] = 1
                    else:
                        tags[key] += 1

for tag in sorted(tags, key=tags.get, reverse=True):
    print tag, tags[tag]
