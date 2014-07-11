# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import codecs
from datetime import datetime
import logging
import json
import os
import urlparse

# Third-party imports
import numpy as np

# Local imports
from author.FeedAuthor import FeedAuthor
from author.FlickrGroup import FlickrGroup
from author.TumblrAuthor import TumblrAuthor
from author.TwitterAuthor import TwitterAuthor

class CrisisCollection(object):
    """Represents an individual crisis collection.

    """
    def __init__(self, blu_pen_collection, collection_country,
                 collection_services=None, collection_types=None):
        """Constructs a CrisisCollection instance.

        """
        # Assign input argument attributes
        self.blu_pen_collection = blu_pen_collection
        self.collection_country = collection_country
        if collection_services is None:
            self.collection_services = ["feed", "flickr", "tumblr", "twitter"]
        else:
            self.collection_services = collection_services
        if collection_types is None:
            self.collection_types = ["common", "crisis"]
        else:
            self.collection_types = collection_types

        # Initialize created attributes
        self.content_dir = os.path.join(self.blu_pen_collection.content_dir, "crisis")
        self.max_dates = 20
        self.percentiles = range(1, 100, 1)
        self.terciles = [25, 75]
        self.collection = {}
        self.collection['sources'] = []
        self.collection['tags'] = {}
        for collection_type in self.collection_types:
            self.collection['tags'][collection_type] = {}

        # Create a logger
        self.logger = logging.getLogger("CrisisCollection")

    def assemble_feed_content(self, collection_type, inp_data):
        """Assembles data and tags for all included feed author.

        """
        # Consider each included feed author
        for author in inp_data['authors']:
            if not author['include']:
                continue

            # Load included feed author content
            try:
                feed_author = FeedAuthor(
                    self.blu_pen_collection.blu_pen_author,
                    author['url'],
                    self.blu_pen_collection.feed_content_dir)
                feed_author.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Assemble data describing included feed author
            data = {}
            data['service'] = "feed"
            data['type'] = collection_type
            data['name'] = author['title']
            data['volume'] = 0
            data['frequency'] = 0
            data['age'] = 0
            data['engagement'] = 0

            # Assemble tags used by included feed author, counting
            # occurrence of each tag
            tags = {}
            for entry in feed_author.entries:
                if entry['content'] == None or not 'tags' in entry['content'][0]:
                    continue
                for tag in entry['content'][0]['tags']:
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Add assembled included feed author content to
            # collection, if tagged
            if len(tags) > 0:
                self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_flickr_content(self, collection_type, inp_data):
        """Assembles data and tags for all included flickr groups.

        """
        # Initialize measurements describing included flickr group
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included flickr group
        for group in inp_data['groups']:
            if not group['include']:
                continue

            # Load included flickr group content
            try:
                flickr_group = FlickrGroup(
                    self.blu_pen_collection.blu_pen_author,
                    u"@" + group['name'],
                    group['nsid'],
                    self.blu_pen_collection.flickr_content_dir)
                flickr_group.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Compute measurements describing included flickr group
            days_fr_upload = []
            for photo in flickr_group.photos:
                days_fr_upload.append(
                    (datetime.today() - datetime.fromtimestamp(float(photo['dateupload']))).days)
            days_fr_upload.sort()
            days_fr_upload = np.array(days_fr_upload)

            # Accumulate measurements describing included flickr group
            volume = np.append(volume, group['photos'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))])))
            age = np.append(age, np.mean(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))]))
            engagement = np.append(engagement, group['members'] / group['photos'])

        # Digitize measurements describing included flickr group
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included flickr group
        i_group = -1
        for group in inp_data['groups']:
            if not group['include']:
                continue
            i_group += 1

            # Assemble data describing included flickr group
            data = {}
            data['service'] = "flickr"
            data['type'] = collection_type
            data['name'] = group['nsid']
            data['volume'] = volume[i_group]
            data['frequency'] = frequency[i_group]
            data['age'] = age[i_group]
            data['engagement'] = engagement[i_group]

            # Assemble tags used by included flickr group, counting
            # occurrence of each tag
            tags = {}
            for photo in flickr_group.photos:
                if not 'tags' in photo:
                    continue
                for tag in photo['tags'].split():
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Add assembled included flickr group content to
            # collection, if tagged
            if len(tags) > 0:
                self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_tumblr_content(self, collection_type, inp_data):
        """Assembles data and tags for all included tumblr author

        """
        # Initialize measurements describing included tumblr author
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included tumblr author
        for author in inp_data['authors']:
            if not author['include']:
                continue

            # Load included tumblr author content
            try:
                tumblr_author = TumblrAuthor(
                    self.blu_pen_collection.blu_pen_author,
                    urlparse.urlparse(author['url']).netloc,
                    self.blu_pen_collection.tumblr_content_dir)
                tumblr_author.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Compute measurements describing included tumblr author
            days_fr_post = []
            for post in tumblr_author.posts:
                days_fr_post.append(
                    (datetime.today() - datetime.fromtimestamp(float(post['timestamp']))).days)
            days_fr_post.sort()
            days_fr_post = np.array(days_fr_post)

            # Accumulate measurements describing included tumblr
            # author
            volume = np.append(volume, author['posts'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_post[0 : min(self.max_dates, len(days_fr_post))])))
            age = np.append(age, np.mean(days_fr_post[0 : min(self.max_dates, len(days_fr_post))]))
            engagement = np.append(engagement, author['likes'] / author['posts'])

        # Digitize measurements describing included tumblr author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included tumblr author
        i_author = -1
        for author in inp_data['authors']:
            if not author['include']:
                continue
            i_author += 1

            # Assemble data describing included tumblr author
            data = {}
            data['service'] = "tumblr"
            data['type'] = collection_type
            data['name'] = author['url']
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble tags used by included tumblr author, counting
            # occurrence of each tag
            tags = {}
            for post in tumblr_author.posts:
                if not 'tags' in post:
                    continue
                for tag in post['tags']:
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Add assembled included tumblr author content to
            # collection, if tagged
            if len(tags) > 0:
                self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_twitter_content(self, collection_type, inp_data):
        """Assembles data and tags for all included tumblr author

        """
        # Initialize measurements describing included tumblr author
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included tumblr author
        for author in inp_data['authors']:
            if not author['include']:
                continue

            # Load included tumblr author content
            try:
                twitter_author = TwitterAuthor(
                    self.blu_pen_collection.blu_pen_author,
                    u"@" + author['screen_name'],
                    self.blu_pen_collection.twitter_content_dir)
                twitter_author.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Compute measurements describing included tumblr author
            days_fr_tweet = []
            for created_dt in twitter_author.created_dt:
                days_fr_tweet.append((datetime.today() - created_dt).days)
            days_fr_tweet.sort()
            days_fr_tweet = np.array(days_fr_tweet)

            # Accumulate measurements describing included tumblr
            # author
            volume = np.append(volume, author['statuses'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))])))
            age = np.append(age, np.mean(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))]))
            engagement = np.append(engagement, author['followers'] / author['statuses'])

        # Digitize measurements describing included tumblr author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included tumblr author
        i_author = -1
        for author in inp_data['authors']:
            if not author['include']:
                continue
            i_author += 1

            # Assemble data describing included tumblr author
            data = {}
            data['service'] = "twitter"
            data['type'] = collection_type
            data['name'] = author['screen_name']
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble tags used by included tumblr author, counting
            # occurrence of each tag
            tags = {}
            for text in twitter_author.clean_text:
                for tag in [token[1:] for token in text.split() if token.startswith('#')]:
                    key = tag # Already unicode
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Add assembled included tumblr author content to
            # collection, if tagged
            if len(tags) > 0:
                self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_content(self):
        """Assembles data and tags for all included author and groups.

        """
        # Consider each collection service
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:

                # Assign name and path of input file containing author
                # content
                inp_file_name = "{0}-{1}-{2}.json".format(
                    collection_service, self.collection_country, collection_type)
                inp_file_path = os.path.join(
                    self.blu_pen_collection.author_requests_dir, 'did-pop',
                    inp_file_name)
                if not os.path.exists(inp_file_path):
                    continue

                # Load input file containing author content
                inp_file = codecs.open(inp_file_path, encoding='utf-8', mode='r')
                inp_data = json.loads(inp_file.read())
                inp_file.close()

                # Assemble author content data
                if inp_data['service'] == "feed":
                    self.assemble_feed_content(collection_type, inp_data)

                if inp_data['service'] == "flickr":
                    self.assemble_flickr_content(collection_type, inp_data)

                elif inp_data['service'] == "tumblr":
                    self.assemble_tumblr_content(collection_type, inp_data)

                elif inp_data['service'] == "twitter":
                    self.assemble_twitter_content(collection_type, inp_data)

        # Initialize assembled collection sources and tags for export
        export = {}
        export['country'] = self.collection_country
        export['sources'] = []
        export['tags'] = []

        # Sort source tags by collection type, and count occurrence of
        # each tag in the collection
        for source in self.collection['sources']:
            source_tags = source['tags']
            collection_type = source['data']['type']
            for source_tag in source_tags:
                if not source_tag in self.collection['tags'][collection_type]:
                    self.collection['tags'][collection_type][source_tag] = 1
                else:
                    self.collection['tags'][collection_type][source_tag] += 1

        # Compute source score as the product of the occurrence of the
        # tag in the source and the occurrence of the tag in the
        # collection
        for source in self.collection['sources']:
            source_tags = source['tags']
            collection_type = source['data']['type']
            source['data']['score'] = 0
            for source_tag in source_tags:
                source['data']['score'] += source_tags[source_tag] * self.collection['tags'][collection_type][source_tag]

        # Identify top sources for each service by score, and append
        # all sources for export
        n_included = 3
        included = {}
        for collection_type in self.collection_types:
            included[collection_type] = {}
            for collection_service in self.collection_services:
                included[collection_type][collection_service] = 0
        for source in sorted(self.collection['sources'], key=lambda source: source['data']['score'], reverse=True):
            collection_type = source['data']['type']
            source_service = source['data']['service']
            source['data']['include'] = False
            if included[collection_type][source_service] < n_included:
                included[collection_type][source_service] += 1
                source['data']['include'] = True
            export['sources'].append(source['data'])

        # Identify top tags by occurrence, and append each for export
        n_tags = 10
        i_tags = {}
        for collection_type in self.collection_types:
            i_tags[collection_type] = 0
        for collection_type in self.collection_types:
            tags = self.collection['tags'][collection_type]
            for key in sorted(tags, key=tags.get, reverse=True):
                tag = {'tag': key, 'type': collection_type, 'count': tags[key]}
                if i_tags[collection_type] < n_tags and not tag in export['tags']:
                    i_tags[collection_type] += 1
                    export['tags'].append(tag)

        # Export assembled collection sources and tags
        out_file_name = "{0}.json".format(self.collection_country)
        out_file_path = os.path.join(self.content_dir, out_file_name)
        out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
        out_file.write(json.dumps(export, ensure_ascii=False, indent=4, separators=(',', ': ')))
        out_file.close()
