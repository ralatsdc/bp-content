# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import codecs
from datetime import datetime
import hashlib
import logging
import json
import os
import pickle
import shutil
import sys
import urlparse

# Third-party imports
import numpy as np
import lucene
from java.io import File
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import DirectoryReader, FieldInfo, IndexWriter, IndexWriterConfig
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

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
    def __init__(self, blu_pen_collection, collection_country, collection_query,
                 collection_services=None, collection_types=None):
        """Constructs a CrisisCollection instance.

        """
        # Assign input argument attributes
        self.blu_pen_collection = blu_pen_collection
        self.feed_utility = FeedUtility()
        self.collection_country = collection_country
        self.collection_query = collection_query
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
        self.documents_dir = os.path.join(self.content_dir, self.collection_country, u"documents")
        self.index_dir = os.path.join(self.content_dir, self.collection_country, u"index")
        self.document_root = os.path.join(u"json", u"source")
        self.pickle_file_name = os.path.join(self.content_dir, self.collection_country, self.collection_country + ".pkl")
        # TODO: Make these parameters
        self.max_dates = 20 # Maximum number of dates on which content is created used for measurements
        self.max_sample = 100 # Maximum number of content samples per author
        self.max_documents = 100 # Maximum number of author documents to score
        self.max_sources = 10 # Maximum number of sources per service and type
        self.max_tags = 10 # Maximum number of tags per service and type
        self.percentiles = range(1, 100, 1)
        self.terciles = [25, 75]
        self.collection = {}
        self.collection['col_data'] = []
        self.collection['col_tags'] = {}
        for collection_service in self.collection_services:
            self.collection[collection_service] = {}
            for collection_type in self.collection_types:
                self.collection[collection_service][collection_type] = {}
                self.collection[collection_service][collection_type]['src_data'] = []
                self.collection[collection_service][collection_type]['src_tags'] = []
                self.collection[collection_service][collection_type]['col_tags'] = {}
        for collection_type in self.collection_types:
            self.collection[collection_type] = {}
            self.collection[collection_type]['col_tags'] = {}

        # Create a logger
        self.logger = logging.getLogger("CrisisCollection")

    def assemble_feed_content(self, collection_type, author_request_data):
        """Assembles data and tags for all included feed authors.

        """
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
                if len(feed_author.entries) == 0:
                    continue
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Assemble data describing included feed author
            data = {}
            data['service'] = "feed"
            data['type'] = collection_type
            data['name'] = author['title']
            data['url'] = author['url']
            json_file_name = feed_author.pickle_file_name.split('/feed/')[1].replace(".pkl", ".json")
            data['json'] = os.path.join(self.document_root, u"feed", json_file_name)
            data['volume'] = 0
            data['frequency'] = 0
            data['age'] = 0
            data['engagement'] = 0

            # Assemble sample content created and tags used by
            # included feed author, counting occurrence of each tag
            sample = []
            tags = {}
            # 'published_parsed'
            # 'updated_parsed'
            # 'created_parsed'
            for entry in sorted(
                    feed_author.entries,
                    key=lambda entry: entry['published_parsed'],
                    reverse=True):
                content = self.feed_utility.get_content(entry)

                # Assemble sample content
                if content == None:
                    continue
                if len(sample) < self.max_sample:
                    # Append text (key 'value' expected in collection
                    # JavaScript)
                    sample.append({'type': "text",
                                   'value': content['value']})
                if 'image_file_names' in content.keys():
                    iIFN = -1
                    for image_file_name in content['image_file_names']:
                        iIFN += 1
                        if len(sample) < self.max_sample:
                            image_url = content['image_urls'][iIFN]
                            image_file_name = os.path.join(self.document_root, u"feed",
                                                           image_file_name.split('/feed/')[1])
                            # And append photos (key 'url' expected in
                            # collection JavaScript)
                            sample.append({'type': "photo",
                                           'url': image_url,
                                           'file_name': image_file_name})

                # Assemble tags
                if not 'tags' in content:
                    continue
                for tag in content['tags']:
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample JSON document for included feed
            # author
            out_file_path = feed_author.pickle_file_name.replace(".pkl", ".json")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Write assembled sample text document for included feed
            # author
            out_file_path = os.path.join(self.documents_dir, u"feed", collection_type, feed_author.source_path + u".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                try:
                    if doc['type'] == "text":
                        out_file.write(doc['value'] + "\n")
                except Exception as exc:
                    pass
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Assign assembled data and tags for included feed author
            # to collection, tagged, or not
            key = hashlib.sha1(out_file_path.encode('utf-8')).hexdigest()
            self.collection[key] = {'data': data, 'tags': tags}
                            

    def assemble_flickr_content(self, collection_type, author_request_data):
        """Assembles data and tags for all included flickr groups.

        """
        # Initialize measurements describing included flickr groups
        groups = [];
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
                if len(flickr_group.photos) == 0:
                    continue
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

            # Accumulate measurements describing included flickr
            # group, and corresponding group
            volume = np.append(volume, group['photos'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))])))
            age = np.append(age, np.mean(days_fr_upload[0 : min(self.max_dates, len(days_fr_upload))]))
            if group['photos'] != 0:
                engagement = np.append(engagement, 1.0 * group['members'] / group['photos'])
            else:
                engagement = np.append(engagement, 0.0);
            groups.append(group)

        # Digitize measurements describing included flickr group
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included flickr group
        i_group = -1
        for group in groups:
            i_group += 1

            # Load included flickr group content
            flickr_group = FlickrGroup(
                self.blu_pen_collection.blu_pen_author,
                u"@" + group['name'],
                group['nsid'],
                self.blu_pen_collection.flickr_content_dir)
            flickr_group.load()

            # Assemble data describing included flickr group
            data = {}
            data['service'] = "flickr"
            data['type'] = collection_type
            data['name'] = group['name']
            data['url'] = u"https://www.flickr.com/groups/" + group['nsid']
            json_file_name = flickr_group.pickle_file_name.split('/flickr/')[1].replace(".pkl", ".json")
            data['json'] = os.path.join(self.document_root, u"flickr", json_file_name)
            data['volume'] = volume[i_group]
            data['frequency'] = frequency[i_group]
            data['age'] = age[i_group]
            data['engagement'] = engagement[i_group]

            # Assemble sample content created and tags used by
            # included flickr group, counting occurrence of each tag
            sample = []
            tags = {}
            def create_flickr_datetime(photo):
                try:
                    photo_dt = datetime.strptime(photo['datetaken'], "%Y-%m-%d %H:%M:%S")
                except Exception as exc:
                    photo_dt = datetime.fromtimestamp(float(photo['dateupload']))
                return photo_dt
            for photo in sorted(flickr_group.photos, key=create_flickr_datetime, reverse=True):
                # Assemble sample content
                if len(sample) < self.max_sample:
                    if 'file_name' in photo.keys():
                        photo_file_name = os.path.join(self.document_root, u"flickr",
                                                       photo['file_name'].split('/flickr/')[1])
                    else:
                        photo_file_name = ""
                    # Append photos (key 'url' expected in collection
                    # JavaScript)
                    sample.append({'type': "photo",
                                   'url': photo['url_m'],
                                   'file_name': photo_file_name,
                                   'title': photo['title']})

                # Assemble tags
                if not 'tags' in photo:
                    continue
                for tag in photo['tags'].split():
                    key = tag # .encode('utf-8')
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample JSON document for included flickr
            # group
            out_file_path = flickr_group.pickle_file_name.replace(".pkl", ".json")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Write assembled sample text document for included flickr
            # group
            out_file_path = os.path.join(self.documents_dir, u"flickr", collection_type, flickr_group.group_id + u".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                out_file.write(doc['title'] + "\n")
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Assign assembled data and tags for included flickr group
            # to collection, tagged, or not
            key = hashlib.sha1(out_file_path.encode('utf-8')).hexdigest()
            self.collection[key] = {'data': data, 'tags': tags}

    def assemble_tumblr_content(self, collection_type, author_request_data):
        """Assembles data and tags for all included tumblr authors.

        """
        # Initialize measurements describing included tumblr author
        authors = []
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
                if len(tumblr_author.posts) == 0:
                    continue
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
            # author, and corresponding author
            volume = np.append(volume, author['posts'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_post[0 : min(self.max_dates, len(days_fr_post))])))
            age = np.append(age, np.mean(days_fr_post[0 : min(self.max_dates, len(days_fr_post))]))
            if author['posts'] != 0:
                engagement = np.append(engagement, 1.0 * author['notes'] / author['posts'])
            else:
                engagement = np.append(engagement, 0.0);
            authors.append(author)

        # Digitize measurements describing included tumblr author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included tumblr author
        i_author = -1
        for author in authors:
            i_author += 1

            # Load included tumblr author content
            tumblr_author = TumblrAuthor(
                self.blu_pen_collection.blu_pen_author,
                urlparse.urlparse(author['url']).netloc,
                self.blu_pen_collection.tumblr_content_dir)
            tumblr_author.load()

            # Assemble data describing included tumblr author
            data = {}
            data['service'] = "tumblr"
            data['type'] = collection_type
            data['name'] = author['name']
            data['url'] = author['url']
            json_file_name = tumblr_author.pickle_file_name.split('/tumblr/')[1].replace(".pkl", ".json")
            data['json'] = os.path.join(self.document_root, u"tumblr", json_file_name)
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble sample content created and tags used by
            # included tumblr author, counting occurrence of each tag
            sample = []
            tags = {}
            for post in sorted(
                    tumblr_author.posts,
                    key=lambda post: datetime.strptime(post['date'].replace(" GMT", ""), "%Y-%m-%d %H:%M:%S"),
                    reverse=True):
                # Assemble sample content
                if len(sample) < self.max_sample:
                    if post['type'] == "text":
                        # Append text (key 'value' expected in
                        # collection JavaScript)
                        sample.append({'type': "text",
                                       'value': post['body']})

                    elif post['type'] == 'photo':
                        if 'alt_sizes_idx' in post['photos'][0]:
                            iAS = post['photos'][0]['alt_sizes_idx']
                        else:
                            iAS = 0
                        photo_url = post['photos'][0]['alt_sizes'][iAS]['url']
                        if 'photo_file_name' in post['photos'][0] and not post['photos'][0]['photo_file_name'] == "":
                            photo_file_name = os.path.join(self.document_root, u"tumblr",
                                                           post['photos'][0]['photo_file_name'].split('/tumblr/')[1])
                        else:
                            photo_file_name = ""
                            
                        # Or append photo (key 'url' expected in
                        # collection JavaScript)
                        sample.append({'type': "photo",
                                       'url': photo_url,
                                       'file_name': photo_file_name})
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

            # Write assembled sample JSON document for included tumblr
            # author
            out_file_path = tumblr_author.pickle_file_name.replace(".pkl", ".json")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Write assembled sample text document for included tumblr
            # author
            out_file_path = os.path.join(self.documents_dir, u"tumblr", collection_type, tumblr_author.subdomain + u".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                if doc['type'] == "text":
                    out_file.write(doc['value'] + "\n")
                # TODO: Add photo caption
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Assign assembled data and tags for included tumblr
            # author to collection, tagged, or not
            key = hashlib.sha1(out_file_path.encode('utf-8')).hexdigest()
            self.collection[key] = {'data': data, 'tags': tags}

    def assemble_twitter_content(self, collection_type, author_request_data):
        """Assembles data and tags for all included twitter authors.

        """
        # Initialize measurements describing included twitter authors
        authors = [];
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
                if len(twitter_author.clean_text) == 0:
                    continue
            except Exception as exc:
                self.logger.error(exc)
                continue

            # Compute measurements describing included twitter author
            days_fr_tweet = []
            for created_dt in twitter_author.created_dt:
                days_fr_tweet.append((datetime.today() - created_dt).days)
            days_fr_tweet.sort()
            days_fr_tweet = np.array(days_fr_tweet)

            # Accumulate measurements describing included twitter
            # author, and corresponding author
            volume = np.append(volume, author['statuses'])
            frequency = np.append(frequency, np.mean(np.diff(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))])))
            age = np.append(age, np.mean(days_fr_tweet[0 : min(self.max_dates, len(days_fr_tweet))]))
            if author['statuses'] != 0:
                engagement = np.append(engagement, 1.0 * author['followers'] / author['statuses'])
            else:
                engagement = np.append(engagement, 0.0)
            authors.append(author)

        # Digitize measurements describing included twitter author
        if len(volume) == 0:
            return
        volume = np.digitize(volume, sorted(np.percentile(volume, self.terciles))) - 1
        frequency = np.digitize(frequency, sorted(np.percentile(frequency, self.percentiles))) - 50
        age = np.digitize(age, sorted(np.percentile(age, self.percentiles))) - 50
        engagement = np.digitize(engagement, sorted(np.percentile(engagement, self.terciles))) - 1

        # Consider each included twitter author
        i_author = -1
        for author in authors:
            i_author += 1

            # Load included twitter author content
            twitter_author = TwitterAuthor(
                self.blu_pen_collection.blu_pen_author,
                u"@" + author['screen_name'],
                self.blu_pen_collection.twitter_content_dir)
            twitter_author.load()

            # Assemble data describing included twitter author
            data = {}
            data['service'] = "twitter"
            data['type'] = collection_type 
            data['name'] = author['name']
            data['url'] = u"https://twitter.com/" + author['screen_name']
            json_file_name = twitter_author.pickle_file_name.split('/twitter/')[1].replace(".pkl", ".json")
            data['json'] = os.path.join(self.document_root, u"twitter", json_file_name)
            data['volume'] = volume[i_author]
            data['frequency'] = frequency[i_author]
            data['age'] = age[i_author]
            data['engagement'] = engagement[i_author]

            # Assemble sample content created and tags used by
            # included twitter author, counting occurrence of each tag
            sample = []
            tags = {}
            created_dt, clean_text = zip(*sorted(zip(
                twitter_author.created_dt, twitter_author.clean_text), reverse=True))
            for text in clean_text:

                # Assemble sample content
                if len(sample) < self.max_sample:
                    # Append text (key 'value' expected in collection
                    # JavaScript)
                    sample.append({'type': "text",
                                   'value': text})

                # Assemble tags
                for tag in [token[1:] for token in text.split() if token.startswith('#')]:
                    key = tag # Already unicode
                    if not key in tags:
                        tags[key] = 1
                    else:
                        tags[key] += 1

            # Write assembled sample JSON document for included
            # twitter author
            out_file_path = twitter_author.pickle_file_name.replace(".pkl", ".json")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            out_file.write(json.dumps(sample, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out_file.close()

            # Write assembled sample text document for included
            # twitter author
            out_file_path = os.path.join(
                self.documents_dir, u"twitter", collection_type, twitter_author.source_path + u".txt")
            out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
            for doc in sample:
                if doc['type'] == "text":
                    out_file.write(doc['value'] + "\n")
            for key in tags.keys():
                out_file.write(key + " ")
            out_file.close()

            # Assign assembled data and tags for included twitter
            # author collection, tagged, or not
            key = hashlib.sha1(out_file_path.encode('utf-8')).hexdigest()
            self.collection[key] = {'data': data, 'tags': tags}

    def assemble_content(self, do_update=False):
        """Assembles data and tags for all included authors and groups.

        """
        # Initialize Lucene
        lucene.initVM(vmargs=["-Djava.awt.headless=true"])
        analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
        store = SimpleFSDirectory(File(self.index_dir))
        
        # Update the assembled and indexed content, if requested
        if not os.path.exists(self.documents_dir) or do_update:

            # == Assemble content

            # Make empty documents directory
            if os.path.exists(self.documents_dir):
                shutil.rmtree(self.documents_dir)
            os.makedirs(self.documents_dir)

            # Consider each collection service
            for collection_service in self.collection_services:

                # Make service documents directory
                os.makedirs(os.path.join(self.documents_dir, collection_service))

                # Consider each collection type
                for collection_type in self.collection_types:

                    # Make service documents directory
                    os.makedirs(os.path.join(self.documents_dir, collection_service, collection_type))

                    # Assign name and path of input file containing
                    # author request
                    author_request_file_name = "{0}-{1}-{2}.json".format(
                        collection_service, self.collection_country, collection_type)
                    author_request_file_path = os.path.join(
                        self.blu_pen_collection.author_requests_dir, u"did-pop",
                        author_request_file_name)
                    if not os.path.exists(author_request_file_path):
                        continue

                    # Load input file containing author request
                    author_request_file = codecs.open(author_request_file_path, encoding='utf-8', mode='r')
                    author_request_data = json.loads(author_request_file.read())
                    author_request_file.close()

                    # Assemble author content, writing sample text and
                    # JSON documents
                    if author_request_data['service'] == "feed":
                        self.assemble_feed_content(collection_type, author_request_data)

                    if author_request_data['service'] == "flickr":
                        self.assemble_flickr_content(collection_type, author_request_data)

                    elif author_request_data['service'] == "tumblr":
                        self.assemble_tumblr_content(collection_type, author_request_data)

                    elif author_request_data['service'] == "twitter":
                        self.assemble_twitter_content(collection_type, author_request_data)

            # == Index content
        
            # Make an empty index directory
            if os.path.exists(self.index_dir):
                shutil.rmtree(self.index_dir)
            os.makedirs(self.index_dir)

            # Initialize an index writer
            # lucene.initVM(vmargs=["-Djava.awt.headless=true"])
            # analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
            # analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
            # store = SimpleFSDirectory(File(self.index_dir))
            config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            writer = IndexWriter(store, config)

            # Define a primary, content field type
            # TODO: Understand these settings
            pf = FieldType()
            pf.setIndexed(True)
            pf.setStored(False)
            pf.setTokenized(True)
            pf.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

            # Define a secondary, decriptive field type
            # TODO: Understand these settings
            sf = FieldType()
            sf.setIndexed(True)
            sf.setStored(True)
            sf.setTokenized(False)
            sf.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)
        
            # Index the documents directory
            for root, dirnames, filenames in os.walk(self.documents_dir):
                head, collection_type = os.path.split(root)
                head, collection_service = os.path.split(head)
                for filename in filenames:
                    if not filename.endswith('.txt'):
                        continue
                    self.logger.info(u"adding {0}".format(filename))
                    try:
                        doc_path = os.path.join(root, filename)
                        doc_file = open(doc_path)
                        contents = unicode(doc_file.read(), 'iso-8859-1')
                        doc_file.close()
                        doc = Document()
                        # print "=1=", collection_service, collection_type, root, filename
                        doc.add(Field("service", collection_service, sf))
                        doc.add(Field("type", collection_type, sf))
                        key = hashlib.sha1(os.path.join(root, filename).encode('utf-8')).hexdigest()
                        doc.add(Field("path", key, sf))
                        doc.add(Field("filename", filename, sf))
                        if len(contents) > 0:
                            doc.add(Field("contents", contents, pf))
                        else:
                            doc.add(Field("contents", "", pf))
                            self.logger.warning(u"no content in {0}".format(filename))
                        writer.addDocument(doc)
                    except Exception as exc:
                        self.logger.error(u"could not index {0}".format(filename))
                        self.logger.error(exc)

            # Commit the index and close the index writer
            writer.commit()
            writer.close()

            # Dump collection pickle
            self.dump()

        else:

            # Load collection pickle
            self.load()

        # == Score content

        # Initialize an index searcher
        # analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
        searcher = IndexSearcher(DirectoryReader.open(store))

        # Consider each collection service
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:

                # Create the query and parser
                if collection_service == "feed":
                    query = ('service:' + collection_service +
                             ' AND type:' + collection_type)
                else:
                    query = ('service:' + collection_service +
                             ' AND type:' + collection_type + 
                             ' AND contents:' + self.collection_query[collection_type])
                parser = QueryParser(Version.LUCENE_CURRENT, "contents", analyzer).parse(query)

                # Score the documents
                scoreDocs = searcher.search(parser, self.max_documents).scoreDocs
                for scoreDoc in scoreDocs:
                    doc = searcher.doc(scoreDoc.doc)
                    # print "=2=",
                    # print 'service: ', doc.get("service"),
                    # print 'type: ', doc.get("type"),
                    # print 'path: ', doc.get("path"),
                    # print 'filename: ', doc.get("filename")
                    # print 'score: ', scoreDoc.score
                    source = self.collection[doc.get('path')]
                    source['data']['score'] = scoreDoc.score
                    self.logger.info(u"scoring {0} with {1}".format(doc.get("filename"), source['data']['score']))

                    # Collect source data and tags for the current collection service and type
                    self.collection[collection_service][collection_type]['src_data'].append(source['data'])
                    self.collection[collection_service][collection_type]['src_tags'].append(source['tags'])

                    # Collect source data for the collection
                    self.collection['col_data'].append(source['data'])

        # == Count tags
        
        # Consider each collection service
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:
                
                # Consider each source tags object
                for src_tags in self.collection[collection_service][collection_type]['src_tags']:
                    
                    # Consider each tag
                    for tag in src_tags:

                        # Count tag occurance for the current collection service and type
                        if not tag in self.collection[collection_service][collection_type]['col_tags']:
                            self.collection[collection_service][collection_type]['col_tags'][tag] = 1
                        else:
                            self.collection[collection_service][collection_type]['col_tags'][tag] += 1
                        
                        # Count tag occurance for the current collection type
                        if not tag in self.collection[collection_type]['col_tags']:
                            self.collection[collection_type]['col_tags'][tag] = 1
                        else:
                            self.collection[collection_type]['col_tags'][tag] += 1

                        # Count tag occurance for the collection
                        if not tag in self.collection['col_tags']:
                            self.collection['col_tags'][tag] = 1
                        else:
                            self.collection['col_tags'][tag] += 1

        # == Assemble collection sources for export

        # Initialize export object
        export = {}
        export['country'] = self.collection_country

        # Consider each collection service
        export['sources'] = []
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:
                
                # Consider each source data object
                n_srcs = 0
                for src_data in sorted(
                        self.collection[collection_service][collection_type]['src_data'],
                        key=lambda data: data['score'],
                        reverse=True):
                    if n_srcs < self.max_sources:
                        n_srcs += 1
                        export['sources'].append(src_data)
                    else:
                        break

        # == Assemble collection tags for export

        # Consider each collection service
        export['tags'] = []
        for collection_service in self.collection_services:

            # Consider each collection type
            for collection_type in self.collection_types:
                
                # Consider each tag object
                n_tags = 0
                for col_tag in sorted(
                        self.collection[collection_service][collection_type]['col_tags'],
                        key=self.collection[collection_service][collection_type]['col_tags'].get,
                        reverse=True):
                    if n_tags < self.max_tags:
                        tag = {'service': collection_service,
                               'type': collection_type,
                               'tag': col_tag,
                               'count': self.collection[collection_service][collection_type]['col_tags'][col_tag]}
                        n_tags += 1
                        export['tags'].append(tag)
                    else:
                        break

        # Consider each collection service
        export['tags'] = []
        # Consider each collection type
        for collection_type in self.collection_types:
            
            # Consider each tag object
            n_tags = 0
            for col_tag in sorted(
                    self.collection[collection_type]['col_tags'],
                    key=self.collection[collection_type]['col_tags'].get,
                    reverse=True):
                if n_tags < self.max_tags:
                    tag = {'type': collection_type,
                           'tag': col_tag,
                           'count': self.collection[collection_type]['col_tags'][col_tag]}
                    n_tags += 1
                    export['tags'].append(tag)
                else:
                    break

        # == Export collection

        # Export assembled collection sources and tags
        out_file_name = "{0}.json".format(
            self.collection_country)
        out_file_path = os.path.join(
            self.content_dir, self.collection_country, out_file_name)
        out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
        out_file.write(json.dumps(export, ensure_ascii=False, indent=4, separators=(',', ': ')))
        out_file.close()
        out_file_name_dated = "{0}-{1}.json".format(
            self.collection_country, datetime.now().strftime('%Y-%m-%d'))
        out_file_path_dated = os.path.join(
            self.content_dir, self.collection_country, out_file_name_dated)
        shutil.copy(out_file_path, out_file_path_dated)

    def dump(self, pickle_file_name=None):
        """Dump collection pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "wb")

        p = {}

        p['collection'] = self.collection

        self.logger.info(u"dumping collection pickle")

        pickle.dump(p, pickle_file)

        pickle_file.close()
        
    def load(self, pickle_file_name=None):
        """Load collection pickle.

        """
        if pickle_file_name == None:
            pickle_file_name  = self.pickle_file_name
        pickle_file = open(pickle_file_name, "rb")

        self.logger.info(u"loading collection pickle")

        p = pickle.load(pickle_file)

        self.collection = p['collection']

        pickle_file.close()

    def old_assemble_content(self):

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
