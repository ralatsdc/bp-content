# -*- coding: utf-8 -*-

# Standard library imports
import random
import simplejson as json
import sys
import unittest

# Third-party imports
import publish.client.client as client

# Local imports
from BluePeninsula import BluePeninsula
from FlickrUtility import FlickrUtility
from TumblrUtility import TumblrUtility
from TwitterUtility import TwitterUtility

class BluePeninsulaTestCase(unittest.TestCase):

    def setUp(self):
        self.blu_pen = BluePeninsula("BluePeninsula.cfg")

    def tearDown(self):
        pass

    def test_random_twitter_users(self):
        """Get random Twitter author screen names, publish each
        author, and confirm that the book was published on Lulu.

        """
        # Set up Twitter
        pclient = client.Client()
        pclient.login("raymond.leclair@thenutcake.net", "st4ndT4ll")
        twitter_utility = TwitterUtility()
        valid_terms = ["animals", "animators", "architecture", "art",
                       "automotive", "book+deals", "books", "comedians", "comics",
                       "creative+writing", "cute", "designers", "developers",
                       "educational", "entertainment", "entrepreneurs",
                       "fashion", "film", "filmmakers", "food", "gaming",
                       "health+fitness", "historical", "humor",
                       "illustrators", "inspiration", "local", "music", "musicians",
                       "nature", "news", "non-profit", "parenting", "personalities",
                       "photographers", "politics", "science", "sports",
                       "startups", "tech", "travel", "typography"]

        # Set up Blue Peninsula
        content = "shortwing"
        do_purge = True
        print_only = True
        notify = True

        # Publish a specified number of Twitter authors
        nSNm = 20
        iSNm = 0
        inp_file = open("BluePeninsula.inp", 'a')
        out_file = open("BluePeninsula.out", 'a')
        while iSNm < nSNm:

            # Get random Twitter authors
            print "\n"
            valid_term = valid_terms[random.randint(0, len(valid_terms) - 1)]
            screen_names = twitter_utility.get_names_from_term(valid_term)

            # Publish each Twitter author
            for screen_name in screen_names:
                iSNm += 1
                inp_file.write(screen_name + "\n")
                out_file.write(screen_name + "...")
                try:
                    result = self.blu_pen.publish_twitter_author(
                        screen_name, content, do_purge, print_only, notify)

                    if not print_only:

                        # Confirm that the book was published on Lulu
                        proj = json.loads(pclient.read(result['id']).to_json())
                        if proj['bibliography']['title'] == result['title']:
                            out_file.write("PASSED Publication\n")
                        else:
                            out_file.write("FAILED Publication\n")

                    else:
                        out_file.write("PASSED Typesetting\n")

                except Exception as exc:
                    message = str(exc)
                    out_file.write("FAILED Typesetting: " + message + "\n")
                    if message.find("Rate limit exceeded.") != -1:
                        break;

                sys.stdout.flush()
                if iSNm == nSNm:
                    break

        inp_file.close()
        out_file.close()

    def test_random_flickr_users(self):
        """Get random Flickr author usernames, publish each author,
        and confirm that the book was published on Lulu.

        """
        # Get random Flickr authors
        print "\n"
        pclient = client.Client()
        pclient.login("raymond.leclair@thenutcake.net", "st4ndT4ll")
        title = "A Memoir"
        flickr_utility = FlickrUtility()
        usernames = flickr_utility.get_names_from_term("happy")

        # Publish each Flickr author
        content = "boulat"
        do_purge = False
        print_only = True
        notify = False
        for username in usernames:
            print username + " ... "
            try:
                id = self.blu_pen.publish_flickr_author(
                    username, content, do_purge, print_only, notify)
                if not print_only:
                    # Confirm that the book was published on Lulu
                    proj = json.loads(pclient.read(id).to_json())
                    if proj['bibliography']['title'] == title:
                        print "ok\n"
                    else:
                        print "FAILED\n"
                else:
                    print "ok\n"

            except Exception as exc:
                print "EXITED"
                print("Couldn't publish flickr author: {0}\n".format(exc))
                f = open("BluePeninsula.inp", 'a')
                f.write(username + "\n")
                f.close()
                break
        
    def test_random_tumblr_users(self):
        """Get random Tumblr author subdomains, publish each author,
        and confirm that the book was published on Lulu.

        """
        # Get random Tumblr authors
        print "\n"
        pclient = client.Client()
        pclient.login("raymond.leclair@thenutcake.net", "st4ndT4ll")
        title = "A Memoir"
        tumblr_utility = TumblrUtility()
        valid_terms = ["animals", "animators", "architecture", "art",
                       "automotive", "book+deals", "books", "comedians", "comics",
                       "creative+writing", "cute", "designers", "developers",
                       "educational", "entertainment", "entrepreneurs",
                       "fashion", "film", "filmmakers", "food", "gaming",
                       "health+fitness", "historical", "humor",
                       "illustrators", "inspiration", "local", "music", "musicians",
                       "nature", "news", "non-profit", "parenting", "personalities",
                       "photographers", "politics", "science", "sports",
                       "startups", "tech", "travel", "typography"]
        valid_term = valid_terms[random.randint(0, len(valid_terms) - 1)]
        subdomains = tumblr_utility.get_names_from_term(valid_term)

        # Publish each Tumblr author
        content = "wheat"
        do_purge = False
        print_only = True
        notify = False
        for subdomain in subdomains:
            print subdomain + " ... "
            try:
                id = self.blu_pen.publish_tumblr_author(
                    subdomain, content, do_purge, print_only, notify)
                if not print_only:
                    # Confirm that the book was published on Lulu
                    proj = json.loads(pclient.read(id).to_json())
                    if proj['bibliography']['title'] == title:
                        print "ok\n"
                    else:
                        print "FAILED\n"
                else:
                    print "ok\n"

            except Exception as exc:
                print "EXITED"
                print("Couldn't publish tumblr author: {0}\n".format(exc))
                f = open("BluePeninsula.inp", 'a')
                f.write(subdomain + "\n")
                f.close()
                break

if __name__ == '__main__':
    # suite = unittest.TestLoader().loadTestsFromTestCase(BluePeninsulaTestCase)
    suite = unittest.TestSuite()
    suite.addTest(BluePeninsulaTestCase("test_random_twitter_users"))
    unittest.TextTestRunner(verbosity=2).run(suite)
