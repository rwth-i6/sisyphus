import logging
from sisyphus import *

DUMP_DIR = 'dump'


def dump(obj, name):
    filename = '%s/%s.pkl' % (DUMP_DIR, name)
    outfile_dir = os.path.dirname(filename)
    if not os.path.isdir(outfile_dir):
        os.makedirs(outfile_dir)
    if os.path.isfile(filename):
        logging.warning("Skip dump since %s is already dump here: %s" % (name, filename))
    else:
        with gzip.open(filename, 'wb') as f:
            pickle.dump(obj, f)


def load(name):
    filename = '%s/%s.pkl' % (DUMP_DIR, name)
    fopen = gzip.open(filename, 'rb') if zipped(filename) else open(filename, 'rb')
    with fopen as f:
        return pickle.load(f)
