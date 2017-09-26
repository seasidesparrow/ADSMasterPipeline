#!/usr/bin/env python

import argparse
import warnings
import json
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
            print json.dumps(r.toJSON(), indent=2, default=str)
        else:
            print 'None'
        print '-' * 80
        
        print 'as seen by SOLR'
        solr_doc = solr_updater.transform_json_record(r.toJSON())
        print json.dumps(solr_doc, indent=2, default=str)
        print '=' * 80
        
def diagnostics(bibcodes):
    """
    Show information about what we have in our storage.
    
    :param: bibcodes - list of bibcodes
    """
    
    if not bibcodes:
        print 'Printing 3 randomly selected records (if any)'
        bibcodes = []
        with app.session_scope() as session:
            for r in session.query(Records).limit(3).all():
                bibcodes.append(r.bibcode)
                
    for b in bibcodes:
        _print_record(b)
        
    
    with app.session_scope() as session:
        print '# of records in db:', session.query(Records.id).count()
    
            



def print_kvs():    
    """Prints the values stored in the KeyValue table."""
    print 'Key, Value from the storage:'
    print '-' * 80
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').yield_per(100):
            print kv.key, kv.value


def reindex(since=None, batch_size=None, force=False, update_solr=True, update_metrics=True):
    """
    Initiates routing of the records (everything that was updated)
    since point in time T.
    """
    if force:
        key = 'last.reindex.forced'
    else:
        key = 'last.reindex.normal'
        
    if update_solr and update_metrics:
        pass # default
    elif update_solr:
        key = key + '.solr-only'
    else:
        key = key + '.metrics-only'
        
    
    previous_since = None
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
                previous_since = since
                kv.value = now.isoformat()
            session.commit()
    else:
        since = get_date(since)
    
    
    logger.info('Sending records changed since: {0}'.format(since.isoformat()))
    ignored = sent = 0
    
    try:
        # select everything that was updated since
        batch = []
        with app.session_scope() as session:
            for rec in session.query(Records) \
                .filter(Records.updated >= since) \
                .options(load_only(Records.bibcode, Records.updated, Records.processed)) \
                .yield_per(100):
                
                if rec.processed and rec.processed > since:
                    ignored += 1
                    continue
                
                sent += 1
                if sent % 1000 == 0:
                    logger.debug('Sending %s records', sent)
                
                if not batch_size or batch_size < 0:
                    batch.append(rec.bibcode)
                elif batch_size > len(batch):
                    batch.append(rec.bibcode)
                else:
                    batch.append(rec.bibcode)
                    tasks.task_index_records.delay(batch, force=force, update_solr=update_solr, update_metrics=update_metrics)
                    batch = []
                
        if len(batch) > 0:
            tasks.task_index_records.delay(batch, force=force, update_solr=update_solr, update_metrics=update_metrics)
        
        logger.info('Done processing %s records (%s were ignored)', sent+ignored, ignored)
    except Exception, e:
        if previous_since:
            logger.error('Failed while submitting data to pipeline, resetting timestamp back to: %s', previous_since)
            with app.session_scope() as session:
                kv = session.query(KeyValue).filter_by(key=key).first()
                kv.value = previous_since
                session.commit()
        else:
            logger.error('Failed while submitting data to pipeline')
        raise e

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
                        dest='since', 
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
                        nargs='?',
                        dest='reindex',
                        action='store',
                        const = 'sm',
                        default='sm',
                        help='Sent all updated documents to SOLR/Postgres (you can combine with --since).' + 
                        'Default is to update both solr and metrics. You can choose what to update.' + 
                        '(s = update solr, m = update metrics)')
    
    parser.add_argument('-e', 
                        '--batch_size', 
                        dest='batch_size', 
                        action='store',
                        default=1000,
                        type=int,
                        help='How many records to process/index in one batch')
    
    args = parser.parse_args()
    
    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')
    
    
    if args.kv:
        print_kvs()

    if args.diagnostics:
        diagnostics(args.bibcodes)
        
    logger.info(args)
    
    if args.reindex:
        update_solr = 's' in args.reindex.lower()
        update_metrics = 'm' in args.reindex.lower()
        reindex(since=args.since, batch_size=args.batch_size, force=args.force, 
                update_solr=update_solr, update_metrics=update_metrics)