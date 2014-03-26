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

class QueueUtility:
    """Provides utilities used by all Blu Pen packages.

    """
    def __init__(self):
        """Constructs a QueueUtility instance.

        """
        self.logger = logging.getLogger("QueueUtility")

    def read_queue(self, request_dir):
        """Read the first-in input file from queue in the specified
        directory, set status, then write the request data to output
        file in did-pop.
        
        """
        # Initialize return value
        inp_file_name = ""
        req_data = {}

        # Handle exceptions to ensure lock file is unlocked
        try:

            # Open and attempt to lock the lock file
            lck_file_name = os.path.join(request_dir, "queue", "queue.lck")
            lck_file = codecs.open(lck_file_name, encoding='utf-8', mode='w')
            is_locked = self.lock(lck_file)
            if not is_locked:
                raise IOError()

            # Find all input files from queue, and return if none
            inp_file_names = glob.glob(os.path.join(request_dir, "queue", "*.json"))
            if len(inp_file_names) == 0:
                raise IOError()

            # Find and read the first-in input file from queue
            inp_file_name = min(inp_file_names, key=os.path.getctime)
            inp_file = codecs.open(inp_file_name, encoding='utf-8', mode='r')
            req_data = json.loads(inp_file.read())
            inp_file.close()

            # Set status and write request data to output file in did-pop
            out_file_name = os.path.basename(inp_file_name)
            self.write_queue(request_dir, out_file_name, req_data, status="doing", queue="did-pop")

            # Remove the input file from queue
            os.remove(inp_file_name)

        except IOError as exc:
            pass

        except Exception as exc:
            self.logger.info(u"Could not read queue: {0}".format(exc))

        finally:

            # Unlock and close the lck file
            if is_locked:
                self.unlock(lck_file)
            lck_file.close()

        return (inp_file_name, req_data)

    def write_queue(self, request_dir, out_file_name, req_data, status="done", queue="did-pop"):
        """Write the request data to the request directory in do-push.
        
        """
        # Set status and write request data to output file in do-push
        req_data['status'] = status
        out_file_name = os.path.join(request_dir, queue, out_file_name)
        out = codecs.open(out_file_name, encoding='utf-8', mode='w')
        out.write(json.dumps(req_data, ensure_ascii=False, indent=4, separators=(',', ': ')))
        out.close()

    def lock(self, lck_file):
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
            self.logger.info(u"Could not lock {0}: {1}".format(lck_file.name, exc))
            return False

        except Exception as exc:
            self.logger.error(u"Could not lock {0}: {1}".format(lck_file.name, exc))
            return False

    def unlock(self, lck_file):
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
