# -*- coding: utf-8 -*-

# Standard library imports
import codecs
import fcntl
import json
import glob
import logging
import os

# Third-party imports

# Local imports

class BluPenUtility:
    """Provides utilities used by all Blu Pen packages.

    """
    def __init__(self)
        """Constructs a BluPenUtility instance.

        """
        self.logger = logging.getLogger(__name__)

    def read_queue(request_dir):
        """Read the first-in input file from queue in the specified
        directory, set status, then write the request data to output
        file in did-pop.
        
        """
        # Open and lock the lck file
        lck_file_name = os.path.join(request_dir, "queue", "queue.lck")
        lck_file = fopen(lck_file_name, 'r')
        is_locked = self.lock(lck_file)
        if is_locked:

            # Find and read the first-in input file from queue
            inp_file_names = glob.glob(os.path.join(request_dir, "queue", "*.json"))
            inp_file_name = min(inp_file_names, key=os.path.getctime)
            inp_file = codecs.open(inp_file_name, encoding='utf-8', mode='r')
            req_data = json.loads(inp.read())
            inp_file.close()

            # Set status and write request data to output file in
            # did-pop
            req_data['status'] = 'doing'
            out_file_name = os.path.join(request_dir, "did-pop", os.basename(inp_file_name))
            out = codecs.open(out_file_name, encoding='utf-8', mode='w')
            out.write(json.dumps(req_data, ensure_ascii=False, indent=4, separators=(',', ': ')))
            out.close()

            # Remove the input file from queue
            os.remove(inp_file_name)

            # Unlock and close the lck file
            self.unlock(lck_file)
            lck_file.close()

        return req_data

    def write_queue(request_dir, out_file_name, req_data):
        """Write the request data to the request directory in do-push.
        
        """
        # Set status and write request data to output file in do-push
        req_data['status'] = 'done'
        out_file_name = os.path.join(request_dir, "do-push", out_file_name)
        out = codecs.open(out_file_name, encoding='utf-8', mode='w')
        out.write(json.dumps(req_data, ensure_ascii=False, indent=4, separators=(',', ': ')))
        out.close()

    def lock(lck_file):
        """Lock the specified file without blocking.

        LOCK_UN – unlock
        LOCK_SH – acquire a shared lock
        LOCK_EX – acquire an exclusive lock
        LOCK_NB – avoid blocking on lock acquisition

        """
        try:
            fcntl.flock(lck_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True

        except IOError as exc:
            self.logger.info(u"Could not lock {0}: {1}".format(lck_file, exc))
            return False

        except Exception as exc:
            self.logger.error(u"Could not lock {0}: {1}".format(lck_file, exc))
            return False

    def unlock(lck_file):
        """Unlock the specified file.

        LOCK_UN – unlock
        LOCK_SH – acquire a shared lock
        LOCK_EX – acquire an exclusive lock
        LOCK_NB – avoid blocking on lock acquisition

        """
        try:
            fcntl.flock(lck_file.fileno(), fcntl.LOCK_UN)
            return True

        except Exception as exc:
            self.logger.error(u"Could not unlock {0}: {1}".format(lck_file, exc))
            return False
