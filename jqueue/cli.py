"""
jqueue CLI
----------

This presents the command line interface to the jqueue server, which accepts
a list of job files, and then waits until all of their jobs have been completed:
"""
import logging
import os
import sys

from jqueue import server

logging.basicConfig(level=logging.INFO)

def main(filenames):
    for filename in filenames:
        if not os.path.isfile(filename):
            print(filename, 'does not exist', file=sys.stderr)
            sys.exit(1)

    svr = server.Server()
    try:
        svr.run([filename.encode('ascii') for filename in filenames])
    except:
        svr.close()
        raise

def runner():
    if '-h' in sys.argv or len(sys.argv) == 1:
        print(sys.argv[0], '<FILE> <FILE> ...', file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1:])
