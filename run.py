#!/usr/bin/env python

import argparse
import warnings
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

from adsputils import setup_logging, get_date
from adsmp.models import KeyValue, Records
from adsmp import tasks, solr_updater
from sqlalchemy.orm import load_only

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


def reindex(since=None, batch_size=None, force=False):
    """
    Initiates routing of the records (everything that was updated)
    since point in time T.
    """
    if force:
        key = 'last.reindex.forced'
    else:
        key = 'last.reindex.normal'
        
    now = get_date()
    if since is None:
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key=key).first()
            if kv is None:
                since = get_date('1972')
                kv = KeyValue(key=key, value=now.isoformat())
                session.add(kv)
            else:
                since = get_date(kv.value)
                kv.value = now.isoformat()
            session.commit()
    else:
        since = get_date(since)
    
    
    logger.info('Sending records changed since: {0}'.format(since.isoformat()))
    ignored = sent = 0
    
    # select everything that was updated since
    batch = []
    with app.session_scope() as session:
        for rec in session.query(Records) \
            .filter(Records.updated >= since) \
            .load_only(Records.bibcode, Records.updated, Records.processed) \
            .all():
            
            if rec.processed and rec.processed > since:
                ignored += 1
                continue
            
            sent += 1
            if sent % 1000 == 0:
                logger.debug('Sending %s records', sent)
            
            if batch_size and batch_size < len(batch):
                batch.append(rec.bibcode)
                continue
            
            tasks.task_index_records.delay(batch)
            batch = []
            
    if len(batch) > 0:
        tasks.task_index_records.delay(batch)
    
    logger.info('Done processing %s records (%s were ignored)', sent+ignored, ignored)
    

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
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='Forces indexing of documents as soon as we receive them.')
    
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
    
    parser.add_argument('-r',
                        '--index',
                        dest='reindex',
                        action='store_true',
                        help='Sent all updated documents to SOLR/Postgres (you can combine with --since)')
    
    parser.add_argument('-b', 
                        '--batch_size', 
                        dest='batch_size', 
                        action='store_true',
                        default=1000,
                        help='How many records to process/index in one batch')
    
    args = parser.parse_args()
    
    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')
    
    
    if args.kv:
        print_kvs()

    if args.diagnostics:
        diagnostics(args.bibcodes)
        
    if args.reindex:
        reindex(since=args.since, batch_size=args.batch_size, force=args.force)