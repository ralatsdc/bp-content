# -*- coding: utf-8 -*-

# Standard library imports
import codecs
import logging
import os

# Third-party imports

# Local imports
from utility.AuthorsUtility import AuthorsUtility

class NotificationUtility(object):
    """Provides notification utilities used by all Blu Pen packages.

    """
    def __init__(self):
        """Constructs a NotificationUtility instance.

        """
        self.host_name = ""
        self.fr_email_address = ""
        self.fr_email_password = ""
        self.logger = logging.getLogger("NotificationUtility")

    def write_email_message(self, source, source_dir, content_dir, file_root, source_str, task_id):
        """Read, update and write email message text and HTML
        templates in oder to create files needed to send a multipart
        email message.

        """
        # Write email message text
        text_file_name = os.path.join(source_dir, file_root + ".text")
        text_file = codecs.open(text_file_name, mode='r', encoding='utf-8', errors='ignore')
        text = text_file.read()
        text_file.close()
        text_file_name = os.path.join(content_dir, file_root + ".text")
        text_file = codecs.open(text_file_name, mode='w', encoding='ascii', errors='ignore')
        if source == "feed":
            text = text.replace("{source_url}", source_str)
            url_at_ep = "http://" + self.host_name + "/feed/status/" + str(task_id) + "/"
        elif source == "flickr":
            text = text.replace("{username}", source_str)
            url_at_ep = "http://" + self.host_name + "/flickr/status/" + str(task_id) + "/"
        elif source == "instagram":
            text = text.replace("{username}", source_str)
            url_at_ep = "http://" + self.host_name + "/instagram/status/" + str(task_id) + "/"
        elif source == "tumblr":
            text = text.replace("{subdomain}", source_str)
            url_at_ep = "http://" + self.host_name + "/tumblr/status/" + str(task_id) + "/"
        elif source == "twitter":
            text = text.replace("{screen_name}", source_str)
            url_at_ep = "http://" + self.host_name + "/twitter/status/" + str(task_id) + "/"
        else:
            raise Exception("Unknown source.")
        text_file.write(text)
        text_file.close()

        # Write email message HTML
        html_file_name = os.path.join(source_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='r', encoding='utf-8', errors='ignore')
        html = html_file.read()
        html_file.close()
        html_file_name = os.path.join(content_dir, file_root + ".html")
        html_file = codecs.open(html_file_name, mode='w', encoding='ascii', errors='ignore')
        if source == "feed":
            html = html.replace("{source_url}", source_str)
        elif source == "flickr":
            html = html.replace("{username}", source_str)
        elif source == "instagram":
            html = html.replace("{username}", source_str)
        elif source == "tumblr":
            html = html.replace("{subdomain}", source_str)
        elif source == "twitter":
            html = html.replace("{screen_name}", source_str)
        html_file.write(html)
        html_file.close()

        return {'text_file_name': text_file_name, 'html_file_name': html_file_name}

    def send_notification(self, to_email_address, subject, text_file_name, html_file_name):
        """Send a text and HTML email message to the recipient from
        Blue Peninsula.

        """
        try:
            au = AuthorsUtility()
            au.send_mail_html(to_email_address,
                              self.fr_email_address,
                              self.fr_email_password,
                              subject,
                              text_file_name,
                              html_file_name)
            self.logger.info("notification sent to {0}".format(
                to_email_address))
        except Exception as exc:
            self.logger.error("notification could not be sent to {0}: {1}".format(
                    to_email_address, exc))

