
from __future__ import division

import codecs
from datetime import datetime
import json
import os
import pprint
import sys
import urlparse

import numpy as np

from authors.BluPenAuthor import BluPenAuthor
from authors.FeedAuthor import FeedAuthor
from authors.FlickrGroup import FlickrGroup
from authors.TumblrAuthor import TumblrAuthor
from authors.TwitterAuthor import TwitterAuthor
from utility.AuthorsUtility import AuthorsUtility

config_file = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/source/authors/BluPenAuthor.cfg"

blu_pen_author = BluPenAuthor(config_file)
authors_utility = AuthorsUtility()

inp_dir_name = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/requests/authors/did-pop"

source_countries = [
    "car",
    "haiti",
    "japan",
    "philippines",
    "south-sudan",
    "syria",
    "zimbabwe"
]

source_types = [
    "common",
    "crisis"
]

max_dates = 20
percentiles = range(1, 100, 1)
deciles = range(10, 100, 10)
terciles = [25, 75]

for country in source_countries:

    collection = {}
    collection['sources'] = []
    collection['common-tags'] = {}
    collection['crisis-tags'] = {}

    for type in source_types:

        inp_file_names = [
            "flickr-{0}-{1}.json".format(country, type),
            "tumblr-{0}-{1}.json".format(country, type),
            "twitter-{0}-{1}.json".format(country, type),
        ]
        if type == "crisis":
            inp_file_names.append("feed-{0}-{1}.json".format(country, type))

        for inp_file_name in inp_file_names:
            inp_file_path = os.path.join(inp_dir_name, inp_file_name)

            if not os.path.exists(inp_file_path):
                continue

            inp_file = codecs.open(inp_file_path, encoding='utf-8', mode='r')
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
                        raise exc
                        
                    """
                    author.keys()
                    [u'url',
                     u'include',
                     u'description',
                     u'title']

                    feed_author.content.keys()
                    ['feed',
                     'status',
                     'updated',
                     'updated_parsed',
                     'encoding',
                     'bozo',
                     'headers',
                     'etag',
                     'href',
                     'version',
                     'entries',
                     'namespaces']

                    feed_author.entries[0].keys()
                    ['publisher',
                     'license',
                     'updated_parsed',
                     'published_parsed',
                     'title',
                     'author',
                     'content',
                     'expired_parsed',
                     'created_parsed']
                    """

                    data = {}

                    data['service'] = "feed"
                    data['type'] = type
                    data['name'] = author['title']
                    data['volume'] = 0
                    data['frequency'] = 0
                    data['age'] = 0
                    data['engagement'] = 0

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
                    
                    if len(tags) > 0:
                        collection['sources'].append({'data': data, 'tags': tags})

            if inp_data['service'] == "flickr":

                volume = np.array([])
                frequency = np.array([])
                age = np.array([])
                engagement = np.array([])

                cur_tags = []

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
                        continue

                    """
                    inp_data['groups'][0].keys()
                    [u'name',
                     u'nsid',
                     u'trusting',
                     u'topic_count',
                     u'photos',
                     u'score',
                     u'members',
                     u'eighteenplus',
                     u'include',
                     u'pool_count']

                    flickr_group.photos[0].keys()
                    ['isfamily',
                     'dateupload', 
                     'dateadded', 
                     'title',
                     'farm',
                     'file_name',
                     'ispublic',
                     'secret',
                     'longitude',
                     'server',
                     'datetaken',
                     'isfriend',
                     'url_m',
                     'ownername',
                     'owner',
                     'latitude',
                     'id',
                     'tags']
                    """

                    days_fr_upload = []
                    for photo in flickr_group.photos:
                        days_fr_upload.append(
                            (datetime.today() - datetime.fromtimestamp(float(photo['dateupload']))).days)
                    days_fr_upload.sort()
                    days_fr_upload = np.array(days_fr_upload)

                    volume = np.append(volume, group['photos'])
                    frequency = np.append(frequency, np.mean(np.diff(days_fr_upload[0 : min(max_dates, len(days_fr_upload))])))
                    age = np.append(age, np.mean(days_fr_upload[0 : min(max_dates, len(days_fr_upload))]))
                    engagement = np.append(engagement, group['members'] / group['photos'])

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

                    cur_tags.append(tags)

                if len(volume) == 0:
                    continue

                volume = np.digitize(volume, sorted(np.percentile(volume, terciles))) - 1
                frequency = np.digitize(frequency, sorted(np.percentile(frequency, percentiles))) - 50
                age = np.digitize(age, sorted(np.percentile(age, percentiles))) - 50
                engagement = np.digitize(engagement, sorted(np.percentile(engagement, terciles))) - 1

                i_group = -1
                for group in inp_data['groups']:
                    if not group['include']:
                        continue
                    i_group += 1

                    data = {}

                    data['service'] = "flickr"
                    data['type'] = type
                    data['name'] = group['nsid']
                    data['volume'] = volume[i_group]
                    data['frequency'] = frequency[i_group]
                    data['age'] = age[i_group]
                    data['engagement'] = engagement[i_group]

                    if len(cur_tags[i_group]) > 0:
                        collection['sources'].append({'data': data, 'tags': cur_tags[i_group]})

            elif inp_data['service'] == "tumblr":

                volume = np.array([])
                frequency = np.array([])
                age = np.array([])
                engagement = np.array([])

                cur_tags = []

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

                    """
                    inp_data['authors'][0].keys()
                    [u'description',
                     u'title',
                     u'url',
                     u'trusting',
                     u'posts',
                     u'score',
                     u'likes',
                     u'include',
                     u'name']

                    tumblr_author.info

                    tumblr_author.posts[0].keys()
                    [u'post_url',
                     u'slug',
                     u'reblog_key',
                     u'format',
                     u'timestamp',
                     u'image_permalink',
                     u'tags',
                     u'blog_name',
                     u'id',
                     u'highlighted',
                     u'photos',
                     u'state',
                     u'converted',
                     u'short_url',
                     u'date',
                     u'caption',
                     u'type',
                     u'note_count',
                     u'link_url']
                    """

                    days_fr_post = []
                    for post in tumblr_author.posts:
                        days_fr_post.append(
                            (datetime.today() - datetime.fromtimestamp(float(post['timestamp']))).days)
                    days_fr_post.sort()
                    days_fr_post = np.array(days_fr_post)

                    volume = np.append(volume, author['posts'])
                    frequency = np.append(frequency, np.mean(np.diff(days_fr_post[0 : min(max_dates, len(days_fr_post))])))
                    age = np.append(age, np.mean(days_fr_post[0 : min(max_dates, len(days_fr_post))]))
                    engagement = np.append(engagement, author['likes'] / author['posts'])

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

                    cur_tags.append(tags)

                if len(volume) == 0:
                    continue

                volume = np.digitize(volume, sorted(np.percentile(volume, terciles))) - 1
                frequency = np.digitize(frequency, sorted(np.percentile(frequency, percentiles))) - 50
                age = np.digitize(age, sorted(np.percentile(age, percentiles))) - 50
                engagement = np.digitize(engagement, sorted(np.percentile(engagement, terciles))) - 1

                i_author = -1
                for author in inp_data['authors']:
                    if not author['include']:
                        continue
                    i_author += 1

                    data = {}

                    data['service'] = "tumblr"
                    data['type'] = type
                    data['name'] = author['url']
                    data['volume'] = volume[i_author]
                    data['frequency'] = frequency[i_author]
                    data['age'] = age[i_author]
                    data['engagement'] = engagement[i_author]

                    if len(cur_tags[i_author]) > 0:
                        collection['sources'].append({'data': data, 'tags': cur_tags[i_author]})

            elif inp_data['service'] == "twitter":

                volume = np.array([])
                frequency = np.array([])
                age = np.array([])
                engagement = np.array([])

                cur_tags = []

                for author in inp_data['authors']:
                    if not author['include']:
                        continue

                    source_words_str = u"@" + author['screen_name']

                    content_dir = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/twitter"

                    try:
                        twitter_author = TwitterAuthor(blu_pen_author, source_words_str, content_dir)
                        twitter_author.load()
                    except Exception as exc:
                        continue

                    """
                    inp_data['authors'][0].keys()
                    [u'statuses_count',
                     u'screen_name',
                     u'created_at',
                     u'description',
                     u'followers_count',
                     u'score',
                     u'followers',
                     u'include',
                     u'trusting',
                     u'statuses',
                     u'name']

                    twitter_author.authors_utility
                    twitter_author.background_rgb
                    twitter_author.blu_pen_author
                    twitter_author.clean_text
                    twitter_author.compute_word_frequency
                    twitter_author.content_dir
                    twitter_author.content_set
                    twitter_author.count
                    twitter_author.created_dt
                    twitter_author.dump
                    twitter_author.frequency
                    twitter_author.get_credentials
                    twitter_author.get_tweets_by_source
                    twitter_author.last_tweets
                    twitter_author.length
                    twitter_author.link_rgb
                    twitter_author.load
                    twitter_author.logger
                    twitter_author.max_id
                    twitter_author.max_length
                    twitter_author.number_of_api_attempts
                    twitter_author.page
                    twitter_author.pickle_file_name
                    twitter_author.seconds_between_api_attempts
                    twitter_author.set_tweets
                    twitter_author.set_tweets_from_archive
                    twitter_author.sidebar_fill_rgb
                    twitter_author.since_id
                    twitter_author.sort_tweets
                    twitter_author.source_header
                    twitter_author.source_label
                    twitter_author.source_log
                    twitter_author.source_path
                    twitter_author.source_types
                    twitter_author.source_words
                    twitter_author.start_date
                    twitter_author.stop_date
                    twitter_author.text_rgb
                    twitter_author.text_symbol
                    twitter_author.tweet_id
                    twitter_author.tweets
                    twitter_author.twitter_start_date
                    twitter_author.twitter_stop_date
                    twitter_author.volume
                    """

                    days_fr_tweet = []
                    for created_dt in twitter_author.created_dt:
                        days_fr_tweet.append((datetime.today() - created_dt).days)
                    days_fr_tweet.sort()
                    days_fr_tweet = np.array(days_fr_tweet)

                    volume = np.append(volume, author['statuses'])
                    frequency = np.append(frequency, np.mean(np.diff(days_fr_tweet[0 : min(max_dates, len(days_fr_tweet))])))
                    age = np.append(age, np.mean(days_fr_tweet[0 : min(max_dates, len(days_fr_tweet))]))
                    engagement = np.append(engagement, author['followers'] / author['statuses'])

                    tags = {}

                    for text in twitter_author.clean_text:
                        for tag in [token[1:] for token in text.split() if token.startswith('#')]:
                            key = tag # Already unicode

                            if not key in tags:
                                tags[key] = 1
                            else:
                                tags[key] += 1

                    cur_tags.append(tags)

                if len(volume) == 0:
                    continue

                volume = np.digitize(volume, sorted(np.percentile(volume, terciles))) - 1
                frequency = np.digitize(frequency, sorted(np.percentile(frequency, percentiles))) - 50
                age = np.digitize(age, sorted(np.percentile(age, percentiles))) - 50
                engagement = np.digitize(engagement, sorted(np.percentile(engagement, terciles))) - 1

                i_author = -1
                for author in inp_data['authors']:
                    if not author['include']:
                        continue
                    i_author += 1

                    data = {}

                    data['service'] = "twitter"
                    data['type'] = type
                    data['name'] = author['screen_name']
                    data['volume'] = volume[i_author]
                    data['frequency'] = frequency[i_author]
                    data['age'] = age[i_author]
                    data['engagement'] = engagement[i_author]

                    if len(cur_tags[i_author]) > 0:
                        collection['sources'].append({'data': data, 'tags': cur_tags[i_author]})

    for source in collection['sources']:
        tag_type = source['data']['type'] + '-tags'
        for tag in source['tags']:
            if not tag in collection[tag_type]:
                collection[tag_type][tag] = 1 # source['tags'][tag]
            else:
                collection[tag_type][tag] += 1 # source['tags'][tag]

    for source in collection['sources']:
        source['data']['score'] = 0
        source['data']['include'] = False
        tag_type = source['data']['type'] + '-tags'
        for tag in source['tags']:
            source['data']['score'] += source['tags'][tag] * collection[tag_type][tag]

    n_included = 3

    included = {}
    included['common'] = {}
    included['common']['feed'] = 0
    included['common']['flickr'] = 0
    included['common']['tumblr'] = 0
    included['common']['twitter'] = 0
    included['crisis'] = {}
    included['crisis']['feed'] = 0
    included['crisis']['flickr'] = 0
    included['crisis']['tumblr'] = 0
    included['crisis']['twitter'] = 0

    for source in sorted(collection['sources'], key=lambda source: source['data']['score'], reverse=True):
        type = source['data']['type']
        service = source['data']['service']
        if included[type][service] < n_included:
            included[type][service] += 1
            source['data']['include'] = True

    out_file_name = "{0}.json".format(country)

    out_dir_name = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/source"
    out_file_path = os.path.join(out_dir_name, out_file_name)

    out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')

    out_file.write(u'{\n')
    out_file.write(u'    "sources": [\n')
    for source in collection['sources']:
        out_file.write(u'        {\n')
        out_file.write(u'            "data": {\n')
        data = source['data']
        for key in data:
            out_file.write(u'                "{0}": "{1}"\n'.format(key, data[key]))
        out_file.write(u'            }\n')
        out_file.write(u'            "tags": {\n')
        tags = source['tags']
        for key in sorted(tags, key=tags.get, reverse=True):
            out_file.write(u'                "{0}": "{1}"\n'.format(key, tags[key]))
        out_file.write(u'            }\n')
        out_file.write(u'        }\n')
    out_file.write(u'    ]\n')
    out_file.write(u'    "common-tags": {\n')
    tags = collection['common-tags']
    for key in sorted(tags, key=tags.get, reverse=True):
        out_file.write(u'        "{0}": "{1}"\n'.format(key, tags[key]))
    out_file.write(u'   }\n')
    out_file.write(u'}\n')
    out_file.write(u'    "crisis-tags": {\n')
    tags = collection['crisis-tags']
    for key in sorted(tags, key=tags.get, reverse=True):
        out_file.write(u'        "{0}": "{1}"\n'.format(key, tags[key]))
    out_file.write(u'   }\n')
    out_file.write(u'}\n')

    out_file.close()

    n_tag = 10

    export = {}
    export['data'] = []
    export['tags'] = {}
    export['tags']['common'] = {}
    export['tags']['crisis'] = {}

    for source in collection['sources']:
        data = source['data']
        tags = source['tags']

        export['data'].append(data)

        if data['include']:
            i_tag = 0
            for tag in sorted(tags, key=tags.get, reverse=True):
                i_tag += 1
                if not tag in export['tags'][data['type']]:
                    export['tags'][data['type']][tag] = tags[tag]
                else:
                    export['tags'][data['type']][tag] += tags[tag]
                if i_tag > n_tag:
                    break

    for type in ['common', 'crisis']:
        tags = export['tags'][type]
        i_tag = 0
        for tag in sorted(tags, key=tags.get, reverse=True):
            i_tag += 1
            if i_tag < n_tag:
                continue
            del tags[tag]

    out_dir_name = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-packages/source/exercise-05/json"
    out_file_path = os.path.join(out_dir_name, out_file_name)

    out_file = codecs.open(out_file_path, encoding='utf-8', mode='w')
    out_file.write(json.dumps(export, ensure_ascii=False, indent=4, separators=(',', ': ')))
    out_file.close()

