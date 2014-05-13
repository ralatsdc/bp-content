# Loads all images given an author request and computes dates

import codecs
import datetime
import json
import os

import numpy as np
import matplotlib.pyplot as plt

from authors.BluPenAuthor import BluPenAuthor
from authors.FlickrGroup import FlickrGroup
from utility.AuthorsUtility import AuthorsUtility

config_file = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/blu-pen/authors/BluPenAuthor.cfg"

blu_pen_author = BluPenAuthor(config_file)

inp_file_name = "flickr.json"
inp_file = codecs.open(inp_file_name, encoding='utf-8', mode='r')
inp_data = json.loads(inp_file.read())
inp_file.close()

days = []

d_0 = datetime.datetime.utcnow()

for group in inp_data['groups']:
    source_word_str = '@' + group['name']

    authors_utility = AuthorsUtility()
    (source_log,
     source_path,
     source_header,
     source_label,
     source_type,
     source_word) = authors_utility.process_source_words(source_word_str)

    content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/flickr"

    group_id = group['nsid']

    try:
        flickr_group = FlickrGroup(blu_pen_author, source_word_str, group_id, content_dir)
        flickr_group.load()
    except Exception as exc:
        next

    for photo in flickr_group.photos:
        try:
            d_t = datetime.datetime.strptime(photo['datetaken'], '%Y-%m-%d %H:%M:%S')
            days.append((d_0 - d_t).days)
        except Exception as exc:
            next
