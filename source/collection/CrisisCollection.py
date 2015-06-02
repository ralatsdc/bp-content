# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import codecs
from datetime import datetime
import logging
import json
import os
import shutil
import sys
import urlparse

# Third-party imports
import numpy as np

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from author.FeedAuthor import FeedAuthor
from author.FeedUtility import FeedUtility
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
        self.feed_utility = FeedUtility()
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
        self.content_dir = os.path.join(self.blu_pen_collection.content_dir, u"crisis")
        self.feed_index_dir = os.path.join(self.content_dir, u"feed")
        self.flickr_index_dir = os.path.join(self.content_dir, u"flickr")
        self.instagram_index_dir = os.path.join(self.content_dir, u"instagram")
        self.tumblr_index_dir = os.path.join(self.content_dir, u"tumblr")
        self.twitter_index_dir = os.path.join(self.content_dir, u"twitter")
        self.document_root = os.path.join(u"json", u"source")
        # TODO: Make these parameters
        self.max_dates = 20
        self.max_sample = 100
        self.percentiles = range(1, 100, 1)
        self.terciles = [25, 75]
        self.collection = {}
        self.collection['sources'] = []
        self.collection['tags'] = {}
        for collection_type in self.collection_types:
            self.collection['tags'][collection_type] = {}

        # Create a logger
        self.logger = logging.getLogger("CrisisCollection")

    def assemble_feed_content(self, collection_type, author_request_data, source_request_data):
        """Assembles data and tags for all included feed authors.

        """
        # Make or remove files in feed index directory
        if os.path.exists(self.feed_index_dir):
            shutil.rmtree(self.feed_index_dir)
        os.makedirs(self.feed_index_dir)

        # Consider each included feed author
        for author in author_request_data['authors']:
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
            data['url'] = author['url']
            json_file_name = feed_author.pickle_file_name.split('/feed/')[1].replace('.pkl', '.json')
            data['json'] = os.path.join(self.document_root, 'feed', json_file_name)
            data['volume'] = 0
            data['frequency'] = 0
            data['age'] = 0
            data['engagement'] = 0

            # Assemble sample content created and tags used by
            # included feed author, counting occurrence of each tag
            sample = []
            tags = {}
            for entry in feed_author.entries:
                content = self.feed_utility.get_content(entry)

                # Assemble sample content
                if content == None:
                    continue
                if len(sample) < self.max_sample:
                    sample.append({'type': 'text',
                                   'value': content['value']}) # Append text (key 'value' expected in collection JavaScript)
                if 'image_file_names' in content.keys():
                    iIFN = -1
                    for image_file_name in content['image_file_names']:
                        iIFN += 1
                        if len(sample) < self.max_sample:
                            photo_url = content['image_urls'][iIFN]
                            photo_file_name = os.path.join(self.document_root, 'feed',
                                                           image_file_name.split('/feed/')[1])
                            sample.append({'type': 'photo',
                                           'url': photo_url,
                                           'file_name': photo_file_name}) # And append photos (key 'url' expected in collection JavaScript)

                # Assemble tags
                if not 'tags' in content:
                    continue
                for tag in content['tags']:
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample text document for included feed
            # author
            out_file_path = os.path.join(self.feed_index_dir, feed_author.source_path + ".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                if doc['type'] == "text":
                    out_file.write(doc['text'] + "\n")
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Write assembled sample JSON document for included feed
            # author
            out_file_path = feed_author.pickle_file_name.replace('.pkl', '.json')
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Append assembled data and tags for included feed author
            # to collection, tagged, or not
            self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_flickr_content(self, collection_type, author_request_data, source_request_data):
        """Assembles data and tags for all included flickr groups.

        """
        # Initialize measurements describing included flickr groups
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included flickr group
        for group in author_request_data['groups']:
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

            # Accumulate pickle file name for and measurements
            # describing included flickr group
            volume = np.append(volume, group['photos'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))])))
            age = np.append(age, np.mean(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))]))
            if group['photos'] != 0:
                engagement = np.append(engagement, 1.0 * group['members'] / group['photos'])
            else:
                engagement = np.append(engagement, 0.0);

        # Digitize measurements describing included flickr group
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Make or remove files in flickr index directory
        if os.path.exists(self.flickr_index_dir):
            shutil.rmtree(self.flickr_index_dir)
        os.makedirs(self.flickr_index_dir)

        # Consider each included flickr group
        i_group = -1
        for group in author_request_data['groups']:
            if not group['include']:
                continue
            i_group += 1

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

            # Assemble data describing included flickr group
            data = {}
            data['service'] = "flickr"
            data['type'] = collection_type
            data['name'] = group['name']
            data['url'] = "https://www.flickr.com/groups/" + group['nsid']
            json_file_name = flickr_group.pickle_file_name.split('/flickr/')[1].replace('.pkl', '.json')
            data['json'] = os.path.join(self.document_root, 'flickr', json_file_name)
            data['volume'] = volume[i_group]
            data['frequency'] = frequency[i_group]
            data['age'] = age[i_group]
            data['engagement'] = engagement[i_group]

            # Assemble sample content created and tags used by
            # included flickr group, counting occurrence of each tag
            sample = []
            tags = {}
            for photo in flickr_group.photos:

                # Assemble sample content
                if len(sample) < self.max_sample:
                    if 'file_name' in photo.keys():
                        photo_file_name = os.path.join(self.document_root, 'flickr',
                                                       photo['file_name'].split('/flickr/')[1])
                    else:
                        photo_file_name = ''
                    sample.append({'type': 'photo',
                                   'url': photo['url_m'],
                                   'file_name': photo_file_name,
                                   'title': photo['title']}) # Append photos (key 'url' expected in collection JavaScript)

                # Assemble tags
                if not 'tags' in photo:
                    continue
                for tag in photo['tags'].split():
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample text document for included flickr
            # group
            out_file_path = os.path.join(self.flickr_index_dir, flickr_group.source_path + ".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                out_file.write(doc['title'] + "\n")
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Write assembled sample JSON document for included flickr
            # group
            out_file_path = flickr_group.pickle_file_name.replace('.pkl', '.json')
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Append assembled data and tags for included flickr group
            # to collection, tagged, or not
            
            self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_tumblr_content(self, collection_type, author_request_data, source_request_data):
        """Assembles data and tags for all included tumblr authors.

        """
        # Initialize measurements describing included tumblr author
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included tumblr author
        for author in author_request_data['authors']:
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

            # Accumulate pickle file name for and measurements
            # describing included tumblr author
            volume = np.append(volume, author['posts'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_post[0 : min(self.max_dates, len(days_fr_post))])))
            age = np.append(age, np.mean(days_fr_post[0 : min(self.max_dates, len(days_fr_post))]))
            if author['posts'] != 0:
                engagement = np.append(engagement, 1.0 * author['notes'] / author['posts'])
            else:
                engagement = np.append(engagement, 0.0);

        # Digitize measurements describing included tumblr author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Make or remove files in tumblr index directory
        if os.path.exists(self.tumblr_index_dir):
            shutil.rmtree(self.tumblr_index_dir)
        os.makedirs(self.tumblr_index_dir)

        # Consider each included tumblr author
        i_author = -1
        for author in author_request_data['authors']:
            if not author['include']:
                continue
            i_author += 1

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

            # Assemble data describing included tumblr author
            data = {}
            data['service'] = "tumblr"
            data['type'] = collection_type
            data['name'] = author['name']
            data['url'] = author['url']
            json_file_name = tumblr_author.pickle_file_name.split('/tumblr/')[1].replace('.pkl', '.json')
            data['json'] = os.path.join(self.document_root, 'tumblr', json_file_name)
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble sample content created and tags used by
            # included tumblr author, counting occurrence of each tag
            sample = []
            tags = {}
            for post in tumblr_author.posts:

                # Assemble sample content
                if len(sample) < self.max_sample:
                    if post['type'] == 'text':
                        sample.append({'type': 'text',
                                       'value': post['body']}) # Append text (key 'value' expected in collection JavaScript)

                    elif post['type'] == 'photo':
                        if 'alt_sizes_idx' in post['photos'][0]:
                            iAS = post['photos'][0]['alt_sizes_idx']
                        else:
                            iAS = 0
                        photo_url = post['photos'][0]['alt_sizes'][iAS]['url']
                        if 'photo_file_name' in post['photos'][0]:
                            photo_file_name = os.path.join(self.document_root, 'tumblr',
                                                           post['photos'][0]['photo_file_name'].split('/tumblr/')[1])
                        else:
                            photo_file_name = ''
                        sample.append({'type': 'photo',
                                       'url': photo_url,
                                       'file_name': photo_file_name}) # Or append photo (key 'url' expected in collection JavaScript)
                                       # TODO: Add photo caption
                                       
                # Assemble tags
                if not 'tags' in post:
                    continue
                for tag in post['tags']:
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample text document for included tumblr
            # author
            out_file_path = os.path.join(self.tumblr_index_dir, tumblr_author.subdomain + ".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                if doc['type'] == "text":
                    out_file.write(doc['value'] + "\n")
                # TODO: Add photo caption
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Write assembled sample JSON document for included tumblr
            # author
            out_file_path = tumblr_author.pickle_file_name.replace('.pkl', '.json')
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Append assembled data and tags for included tumblr
            # author to collection, tagged, or not
            self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_twitter_content(self, collection_type, author_request_data, source_request_data):
        """Assembles data and tags for all included twitter authors.

        """
        # Initialize measurements describing included twitter authors
        volume = np.array([])
        frequency = np.array([])
        age = np.array([])
        engagement = np.array([])

        # Consider each included twitter author
        for author in author_request_data['authors']:
            if not author['include']:
                continue

            # Load included twitter author content
            try:
                twitter_author = TwitterAuthor(
                    self.blu_pen_collection.blu_pen_author,
                    u"@" + author['screen_name'],
                    self.blu_pen_collection.twitter_content_dir)
                twitter_author.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Compute measurements describing included twitter author
            days_fr_tweet = []
            for created_dt in twitter_author.created_dt:
                days_fr_tweet.append((datetime.today() - created_dt).days)
            days_fr_tweet.sort()
            days_fr_tweet = np.array(days_fr_tweet)

            # Accumulate pickle file name for and measurements
            # describing included twitter author
            volume = np.append(volume, author['statuses'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))])))
            age = np.append(age, np.mean(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))]))
            if author['statuses'] != 0:
                engagement = np.append(engagement, 1.0 * author['followers'] / author['statuses'])
            else:
                engagement = np.append(engagement, 0.0)

        # Digitize measurements describing included twitter author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Make or remove files in twitter index directory
        if os.path.exists(self.twitter_index_dir):
            shutil.rmtree(self.twitter_index_dir)
        os.makedirs(self.twitter_index_dir)

        # Consider each included twitter author
        i_author = -1
        for author in author_request_data['authors']:
            if not author['include']:
                continue
            i_author += 1

            # Load included twitter author content
            try:
                twitter_author = TwitterAuthor(
                    self.blu_pen_collection.blu_pen_author,
                    u"@" + author['screen_name'],
                    self.blu_pen_collection.twitter_content_dir)
                twitter_author.load()
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Assemble data describing included twitter author
            data = {}
            data['service'] = "twitter"
            data['type'] = collection_type
            data['name'] = author['name']
            data['url'] = "https://twitter.com/" + author['screen_name']
            json_file_name = twitter_author.pickle_file_name.split('/twitter/')[1].replace('.pkl', '.json')
            data['json'] = os.path.join(self.document_root, 'twitter', json_file_name)
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble sample content created and tags used by
            # included twitter author, counting occurrence of each tag
            sample = []
            tags = {}
            for text in twitter_author.clean_text:

                # Assemble sample content
                if len(sample) < self.max_sample:
                    sample.append({'type': 'text',
                                   'value': text}) # Append text (key 'value' expected in collection JavaScript)

                # Assemble tags
                for tag in [token[1:] for token in text.split() if token.startswith('#')]:
                    key = tag # Already unicode
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample text document for included
            # twitter author
            out_file_path = os.path.join(self.twitter_index_dir, twitter_author.source_path + ".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                if doc['type'] == "text":
                    out_file.write(doc['value'] + "\n")
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Write assembled sample JSON document for included
            # twitter author
            out_file_path = twitter_author.pickle_file_name.replace('.pkl', '.json')
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Append assembled data and tags for included twitter
            # author collection, tagged, or not
            self.collection['sources'].append({'data': data, 'tags': tags})

    def assemble_content(self):
        """Assembles data and tags for all included author and groups.

        """
        # Consider each collection service
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:

                # Assign name and path of input file containing source
                # request
                source_request_file_name = "{0}-{1}-{2}.json".format(
                    collection_service, self.collection_country, collection_type)
                source_request_file_path = os.path.join(
                    self.blu_pen_collection.source_requests_dir, 'did-pop',
                    source_request_file_name)
                if not os.path.exists(source_request_file_path):
                    continue

                # Load input file containing source request
                source_request_file = codecs.open(source_request_file_path, encoding='utf-8', mode='r')
                source_request_data = json.loads(source_request_file.read())
                source_request_file.close()

                # Assign name and path of input file containing author
                # request
                author_request_file_name = "{0}-{1}-{2}.json".format(
                    collection_service, self.collection_country, collection_type)
                author_request_file_path = os.path.join(
                    self.blu_pen_collection.author_requests_dir, 'did-pop',
                    author_request_file_name)
                if not os.path.exists(author_request_file_path):
                    continue

                # Load input file containing author request
                author_request_file = codecs.open(author_request_file_path, encoding='utf-8', mode='r')
                author_request_data = json.loads(author_request_file.read())
                author_request_file.close()

                # Assemble author content
                if author_request_data['service'] == "feed":
                    self.assemble_feed_content(collection_type, author_request_data, source_request_data)

                if author_request_data['service'] == "flickr":
                    self.assemble_flickr_content(collection_type, author_request_data, source_request_data)

                elif author_request_data['service'] == "tumblr":
                    self.assemble_tumblr_content(collection_type, author_request_data, source_request_data)

                elif author_request_data['service'] == "twitter":
                    self.assemble_twitter_content(collection_type, author_request_data, source_request_data)

        # Initialize assembled collection sources and tags for export
        export = {}
        export['country'] = self.collection_country
        export['sources'] = []
        export['tags'] = []



        # source['data']['include'] = True
        # export['sources'].append(source['data'])

        # tag = {'tag': collection_tag, 'type': collection_type, 'count': collection_tags[collection_tag]}
        # export['tags'].append(tag)
        
        # for source in self.collection['sources']:
        #     source_tags = source['tags']
        #     source_data = source['data']



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
        # data from all sources for export
        n_included = 5
        included = {}
        for collection_type in self.collection_types:
            included[collection_type] = {}
            for collection_service in self.collection_services:
                included[collection_type][collection_service] = 0
        for source in sorted(self.collection['sources'], key=lambda source: source['data']['score'], reverse=True):
            collection_type = source['data']['type']
            collection_service = source['data']['service']
            if collection_type == "crisis":
                included[collection_type][collection_service] += 1
                source['data']['include'] = True
            else:
                source['data']['include'] = False
                if included[collection_type][collection_service] < n_included:
                    included[collection_type][collection_service] += 1
                    source['data']['include'] = True
            export['sources'].append(source['data'])

        # Identify top tags by occurrence, and append each for export
        n_tags = 10
        i_tags = {}
        for collection_type in self.collection_types:
            i_tags[collection_type] = 0
        for collection_type in self.collection_types:
            collection_tags = self.collection['tags'][collection_type]
            for collection_tag in sorted(collection_tags, key=collection_tags.get, reverse=True):
                tag = {'tag': collection_tag, 'type': collection_type, 'count': collection_tags[collection_tag]}
                if i_tags[collection_type] < n_tags and not tag in export['tags']:
                    i_tags[collection_type] += 1
                    export['tags'].append(tag)

        # Export assembled collection sources and tags
        out_file_name = "{0}.json".format(self.collection_country)
        out_file_path = os.path.join(self.content_dir, out_file_name)
        out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
        out_file.write(json.dumps(export, ensure_ascii=False, indent=4, separators=(',', ': ')))
        out_file.close()
