#!/usr/bin/env python

import sys
import time
import argparse
import logging
import traceback
import requests
import warnings
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

from adsputils import setup_logging, get_date
from adsmp.models import KeyValue, Records
from adsmp import tasks, solr_updater

app = tasks.app
logger = setup_logging('run.py')


def _print_record(bibcode):
    with app.session_scope() as session:
        print 'stored by us:', bibcode
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r:
            print r.toJSON()
        else:
            print 'None'
        print '-' * 80
        
        print 'as seen by SOLR'
        solr_doc = solr_updater.transform_json_record(r.toJSON())
        print solr_doc
        print '=' * 80
        
def diagnostics(bibcodes):
    """
    Show information about what we have in our storage.
    
    :param: bibcodes - list of bibcodes
    """
    if bibcodes:
        for b in bibcodes:
            _print_record(b)
    else:
        'Printing 3 randomly selected records (if any)'
        with app.session_scope() as session:
            for r in session.query(Records).limit(3).all():
                _print_record(r.bibcode)
    
    with app.session_scope() as session:
        print '# of records in db:', session.query(Records.id).count()
    
            



def print_kvs():    
    """Prints the values stored in the KeyValue table."""
    print 'Key, Value from the storage:'
    print '-' * 80
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').all():
            print kv.key, kv.value


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-d',
                        '--diagnostics',
                        dest='diagnostics',
                        action='store_true',
                        help='Show diagnostic message')
    
    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        help='List of bibcodes separated by spaces')
    
    parser.add_argument('-f',
                        '--refetch_orcidids',
                        dest='refetch_orcidids',
                        action='store_true',
                        help='Gets all orcidids changed since X (as discovered from ads api) and sends them to the queue.')
    
    parser.add_argument('-s', 
                        '--since', 
                        dest='since_date', 
                        action='store',
                        default=None,
                        help='Starting date for reindexing')
    
    parser.add_argument('-k', 
                        '--kv', 
                        dest='kv', 
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')
    
    args = parser.parse_args()
    
    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')
    
    
    if args.kv:
        print_kvs()

    if args.diagnostics:
        diagnostics(args.bibcodes)