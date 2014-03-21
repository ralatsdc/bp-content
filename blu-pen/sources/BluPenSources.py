#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import ConfigParser
import argparse
import codecs
import json
import logging
import os
import re
import sys

# Third-party imports
import numpy as np

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from BluPenUtility import BluPenUtility
from FlickrSources import FlickrSources
from TumblrSources import TumblrSources
from TwitterSources import TwitterSources

class BluPenSources:
    """TODO: Complete

    """
    def __init__(self, config_file):
        """Constructs a BluPenSources instance given a configuration
        file and source word.

        """
        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)
        self.sources_request_dir = self.config.get("sources", "request_dir")
        self.authors_request_dir = self.config.get("authors", "request_dir")

        # Create a logger
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        for handler in root.handlers:
            root.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)
        file_handler = logging.FileHandler("BluPenSources.log", mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        self.logger = logging.getLogger(u"BluPenSources")

    def get_sources_from_flickr(self, config_file, source_word_strs):
        """Select a collection of Flickr groups by searching for
        groups using a query term.

        """
        # Consider each source word string
        nsid = []
        name = []
        eighteenplus = []
        members = []
        pool_count = []
        topic_count = []
        description = []
        for source_word_str in source_word_strs:

            # Create and dump, or load, the FlickrSources pickle
            fs = FlickrSources(config_file, source_word_str)
            fs.set_sources()

            # Accumulate arrays of values for selecting groups
            nsid.extend(fs.nsid)
            name.extend(fs.name)
            eighteenplus.extend(fs.eighteenplus)
            members.extend(fs.members)
            pool_count.extend(fs.pool_count)
            topic_count.extend(fs.topic_count)
            description.extend(fs.description)

        # Compute z-scores based on number of photos, number of members,
        # and the members to photos ratio
        n_photos = np.array(pool_count)
        n_members = np.array(members)
        n_trusting = n_members / n_photos

        # Convert the numeric scores to string scores
        s_photos = fs.n_to_s(n_photos)
        s_members = fs.n_to_s(n_members)
        s_trusting = fs.n_to_s(n_trusting)

        # Create a dictionary of groups in order to print a JSON document
        # to a file
        groups = []
        n_grp = len(nsid)
        for i_grp in range(n_grp):
            group = {}
            group['nsid'] = nsid[i_grp]
            group['name'] = name[i_grp]
            group['eighteenplus'] = eighteenplus[i_grp]
            group['members'] = members[i_grp]
            group['pool_count'] = pool_count[i_grp]
            group['topic_count'] = topic_count[i_grp]
            group['photos'] = n_photos[i_grp]
            group['members'] = n_members[i_grp]
            group['trusting'] = n_trusting[i_grp]
            group['score'] = s_photos[i_grp] + s_members[i_grp] + s_trusting[i_grp]
            if group['score'] == "+++":
                group['include'] = True
            else:
                group['include'] = False
            groups.append(group)

        return groups

    def get_sources_from_tumblr(self, config_file, source_word_strs):
        """Selects a set of Tumblr blogs by getting tagged posts.

        """
        # Consider each source word string
        host_names = []
        blog_posts = []
        for source_word_str in source_word_strs:

            # Create a TumblrSources instance, and create the content directory, if needed
            ts = TumblrSources(args.config_file, source_word_str)

            # Create and dump, or load, the TumblrSources pickle.
            ts.set_sources()

            # Accumulate blog info, and posts
            for b_p in ts.blog_posts:
                if not 'blog' in b_p:
                    continue
                h_n = b_p['blog']['name']
                if not h_n in host_names:
                    host_names.append(h_n)
                    blog_posts.append(b_p)

        # Consider sample posts from each blog
        total_tags = []
        for blog in blog_posts:

            # If there are no posts for the current blog, note that the
            # total number of tag appearances is zero, and continue to the
            # next blog
            n_tags = 0
            if not 'posts' in blog:
                total_tags.append(n_tags)
                continue

            # Consider each post from the current blog
            posts = blog['posts']
            for post in posts:

                # Consider each source word
                for source_word_str in source_word_strs:

                    # Process the source word string to create log and
                    # path strings, and assign input argument attributes
                    (source_log,
                     source_path,
                     source_header,
                     source_label,
                     source_type,
                     source_word) = ts.blu_pen_utl.process_source_words(source_word_str)

                    # Count the appearances of the current source word in
                    # the curren post of the current blog
                    n_tags += len(re.findall(source_word, "".join(post['tags']), re.I))

            # Note the total number of tag appearances for the current
            # blog
            total_tags.append(n_tags)

        # Find the blogs with the highest number of tag appearances
        # TODO: Remove the hard coded values
        np_total_tags = np.array(total_tags)
        min_total_tags = 40
        index_blog, = np.nonzero(np_total_tags > min_total_tags)
        while np.size(index_blog) < 100 and min_total_tags > 0:
            min_total_tags -= 1
            index_blog, = np.nonzero(np_total_tags > min_total_tags)

        # Select the blogs with the highest number of tag appearances
        blogs_info = []
        posts = []
        likes = []
        for i_blg in index_blog:
            info = blog_posts[i_blg]['blog']
            blogs_info.append(info)
            if 'posts' in info:
                posts.append(info['posts'])
            else:
                posts.append(0)
            if 'likes' in info:
                likes.append(info['likes'])
            else:
                likes.append(0)

        # Compute scores based on number of posts, number of likes,
        # and the likes to posts ratio
        np_n_posts = np.array(posts)
        np_n_likes = np.array(likes)
        np_n_trusting = np_n_likes / np_n_posts

        # Convert the numeric scores to string scores
        np_s_posts = ts.n_to_s(np_n_posts)
        np_s_likes = ts.n_to_s(np_n_likes)
        np_s_trusting = ts.n_to_s(np_n_trusting)

        # Create a dictionary of blogs in order to print a JSON document
        # to a file
        blogs = []
        for i_blg in range(len(blogs_info)):
            blog = {}

            info = blogs_info[i_blg]

            if 'name' in info:
                blog['name'] = info['name']
            else:
                blog['name'] = ""
            if 'title' in info:
                blog['title'] = info['title']
            else:
                blog['title'] = ""
            if 'description' in info:
                blog['description'] = info['description']
            else:
                blog['description'] = ""
            if 'url' in info:
                blog['url'] = info['url']
            else:
                blog['url'] = ""

            blog['posts'] = np_n_posts[i_blg]
            blog['likes'] = np_n_likes[i_blg]
            blog['trusting'] = np_n_trusting[i_blg]
            blog['score'] = np_s_posts[i_blg] + np_s_likes[i_blg] + np_s_trusting[i_blg]

            if blog['score'] == "+++":
                blog['include'] = True
            else:
                blog['include'] = False

            blogs.append(blog)

        return blogs

    def get_sources_from_twitter(self, config_file, source_word_strs):
        """Selects a collection of Twitter users by searching for
        users using a query term.

        """
        # Consider each source word string
        name = []
        description = []
        screen_name = []
        created_at = []
        statuses_count = []
        followers_count = []
        for source_word_str in source_word_strs:

            # Create and dump, or load, the TwitterSources pickle
            ts = TwitterSources(args.config_file, source_word_str)
            ts.set_sources()

            # Accumulate created atributes
            name.extend(ts.name)
            description.extend(ts.description)
            screen_name.extend(ts.screen_name)
            created_at.extend(ts.created_at)
            statuses_count.extend(ts.statuses_count)
            followers_count.extend(ts.followers_count)

        # Compute scores based on number of statuses, number of
        # followers, and the followers to statuses ratio
        n_statuses = np.array(statuses_count)
        n_followers = np.array(followers_count)
        n_trusting = n_followers / n_statuses

        # Convert the numeric scores to string scores
        s_statuses = ts.n_to_s(n_statuses)
        s_followers = ts.n_to_s(n_followers)
        s_trusting = ts.n_to_s(n_trusting)

        # Create a dictionary of users in order to print a JSON document
        # to a file
        users = []
        n_usr = len(name)
        for i_usr in range(n_usr):
            user = {}
            user['name'] = name[i_usr]
            user['description'] = description[i_usr]
            user['screen_name'] = screen_name[i_usr]
            user['created_at'] = created_at[i_usr]
            user['statuses_count'] = statuses_count[i_usr]
            user['followers_count'] = followers_count[i_usr]
            user['statuses'] = n_statuses[i_usr]
            user['followers'] = n_followers[i_usr]
            user['trusting'] = n_trusting[i_usr]
            user['score'] = s_statuses[i_usr] + s_followers[i_usr] + s_trusting[i_usr]
            if user['score'] == "+++":
                user['include'] = True
            else:
                user['include'] = False
            users.append(user)

        return users
if __name__ == "__main__":
    """TODO: Complete
        
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Selects a collection of sources")
    parser.add_argument("-c", "--config-file",
                        default="BluPenSources.cfg",
                        help="the configuration file")
    args = parser.parse_args()

    # Load the source words JSON document
    bpu = BluPenUtility()
    bps = BluPenSources(args.config_file)
    inp_file_name, req_data = bpu.read_queue(bps.sources_request_dir)

    # Get sources from the specified service
    if req_data['service'] == 'flickr':
        sources = bps.get_sources_from_flickr(args.config_file, req_data['words'])

    elif req_data['service'] == 'tumblr':
        sources = bps.get_sources_from_tumblr(args.config_file, req_data['words'])

    elif req_data['service'] == 'twitter':
        sources = bps.get_sources_from_twitter(args.config_file, req_data['words'])

    # Dump the selected sources JSON document
    out_file_name = os.basename(inp_file_name)
    bpu.write_queue(bps.authors_request_dir, out_file_name, req_data)
