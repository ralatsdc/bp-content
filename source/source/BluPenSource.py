#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard library imports
from __future__ import division
import ConfigParser
import argparse
import logging
import logging.handlers
import os
import re
import sys

# Third-party imports
import numpy as np

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from source.FlickrSource import FlickrSource
from source.TumblrSource import TumblrSource
from source.TwitterSource import TwitterSource
from utility.QueueUtility import QueueUtility

class BluPenSource:
    """Selects source from selected services.

    """
    def __init__(self, config_file):
        """Constructs a BluPenSource instance given a configuration
        file and source word.

        """
        # Parse configuration file
        self.config_file = config_file
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.config_file)

        self.do_purge = self.config.getboolean("source", "do_purge")
        self.source_requests_dir = self.config.get("source", "requests_dir")

        self.add_console_handler = self.config.getboolean("source", "add_console_handler")
        self.add_file_handler = self.config.getboolean("source", "add_file_handler")
        self.log_file_name = self.config.get("source", "log_file_name")
        log_level = self.config.get("source", "log_level")
        if log_level == 'DEBUG':
            self.log_level = logging.DEBUG
        elif log_level == 'INFO':
            self.log_level = logging.INFO
        elif log_level == 'WARNING':
            self.log_level = logging.WARNING
        elif log_level == 'ERROR':
            self.log_level = logging.ERROR
        elif log_level == 'CRITICAL':
            self.log_level = logging.CRITICAL

        self.author_requests_dir = self.config.get("author", "requests_dir")

        self.flickr_content_dir = self.config.get("flickr", "content_dir")
        self.tumblr_content_dir = self.config.get("tumblr", "content_dir")
        self.twitter_content_dir = self.config.get("twitter", "content_dir")

        self.tumblr_min_total_tags = self.config.getint("tumblr", "min_total_tags")
        self.tumblr_min_total_blogs = self.config.getint("tumblr", "min_total_blogs")

        # Create a logger
        root = logging.getLogger()
        root.setLevel(self.log_level)
        formatter = logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        for handler in root.handlers:
            root.removeHandler(handler)
        if self.add_console_handler:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root.addHandler(console_handler)
        if self.add_file_handler:
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file_name, maxBytes=1000000, backupCount=5, encoding='utf-8')
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)

        self.logger = logging.getLogger(u"BluPenSource")

    def get_source_from_flickr(self, source_word_strs, content_dir):
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

            # Create and dump, or load, the FlickrSource pickle
            fs = FlickrSource(source_word_str, content_dir)
            fs.set_source(do_purge=self.do_purge)

            # Accumulate arrays of values for selecting groups
            if not fs.nsid in nsid:
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

    def get_source_from_tumblr(self, source_word_strs, content_dir):
        """Selects a set of Tumblr blogs by getting tagged posts.

        """
        # Consider each source word string
        host_names = []
        blog_posts = []
        for source_word_str in source_word_strs:

            # Create and dump, or load, the TumblrSource pickle.
            ts = TumblrSource(source_word_str, content_dir)
            ts.set_source(do_purge=self.do_purge)

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
                     source_word) = ts.author_utility.process_source_words(source_word_str)

                    # Count the appearances of the current source word in
                    # the curren post of the current blog
                    n_tags += len(re.findall(source_word, "".join(post['tags']), re.I))

            # Note the total number of tag appearances for the current
            # blog
            total_tags.append(n_tags)

        # Find the blogs with the highest number of tag appearances
        np_total_tags = np.array(total_tags)
        min_total_tags = self.tumblr_min_total_tags
        index_blog, = np.nonzero(np_total_tags > min_total_tags)
        while np.size(index_blog) < self.tumblr_min_total_blogs and min_total_tags > 0:
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

    def get_source_from_twitter(self, source_word_strs, content_dir):
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

            # Create and dump, or load, the TwitterSource pickle
            ts = TwitterSource(source_word_str, content_dir)
            ts.set_source(do_purge=self.do_purge)

            # Accumulate created atributes
            if not ts.screen_name in screen_name:
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
    """Process the source queue.
        
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Selects a collection of source")
    parser.add_argument("-c", "--config-file",
                        default="BluPenSource.cfg",
                        help="the configuration file")
    parser.add_argument("-p", "--do-purge",
                        action="store_true",
                        help="purge existing source")
    args = parser.parse_args()
    
    # Read the input request JSON document from source/queue
    qu = QueueUtility()
    bps = BluPenSource(args.config_file)
    bps.do_purge = args.do_purge
    inp_file_name, inp_req_data = qu.read_queue(bps.source_requests_dir)
    out_file_name = os.path.basename(inp_file_name); out_req_data = {}
    if inp_file_name == "" or inp_req_data == {}:
        bps.logger.info("Nothing to do, exiting")
        sys.exit()

    # Get source from the specified service
    if inp_req_data['service'] == 'flickr':
        out_req_data['service'] = 'flickr'
        out_req_data['groups'] = bps.get_source_from_flickr(inp_req_data['words'], bps.flickr_content_dir)

    elif inp_req_data['service'] == 'tumblr':
        out_req_data['service'] = 'tumblr'
        out_req_data['author'] = bps.get_source_from_tumblr(inp_req_data['words'], bps.tumblr_content_dir)

    elif inp_req_data['service'] == 'twitter':
        out_req_data['service'] = 'twitter'
        out_req_data['author'] = bps.get_source_from_twitter(inp_req_data['words'], bps.twitter_content_dir)

    # Write the input request JSON document to source/did-pop
    qu.write_queue(bps.source_requests_dir, out_file_name, inp_req_data)

    # Write the output request JSON document to author/do-push
    qu.write_queue(bps.author_requests_dir, out_file_name, out_req_data, status="todo", queue="do-push")
