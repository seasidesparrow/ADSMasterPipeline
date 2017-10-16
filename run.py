#!/usr/bin/env python

import argparse
import warnings
import json
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)
import time
import requests
from difflib import SequenceMatcher

from adsputils import setup_logging, get_date
from adsmp.models import KeyValue, Records
from adsmp import tasks, solr_updater
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import InstrumentedAttribute

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
        for x in dir(Records):
            if isinstance(getattr(Records, x), InstrumentedAttribute):
                print '# of %s' % x, session.query(Records).filter(getattr(Records, x) != None).count()

    print 'sending test bibcodes to the queue for reindexing'
    tasks.task_index_records.delay(bibcodes, force=True, update_solr=True, update_metrics=True)



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
    last_bibcode = None
    
    try:
        # select everything that was updated since
        batch = []
        with app.session_scope() as session:
            for rec in session.query(Records) \
                .filter(Records.updated >= since) \
                .options(load_only(Records.bibcode, Records.updated, Records.processed)) \
                .yield_per(100):
                
                if rec.processed and rec.processed < since:
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
                    last_bibcode = rec.bibcode
                
        if len(batch) > 0:
            tasks.task_index_records.delay(batch, force=force, update_solr=update_solr, update_metrics=update_metrics, 
                                           commit=force)
        elif force and last_bibcode:
            # issue one extra call with the commit
            tasks.task_index_records.delay([last_bibcode], force=force, update_solr=update_solr, update_metrics=update_metrics, 
                                           commit=force)
        
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

def compare_solr(validation_logger, bibcodelist=None,filename=None):

    if (bibcodelist is None) & (filename is None):
        validation_logger.error('Must pass in a list of bibcodes or a file of bibcodes')

    SOLR_OLD = app.conf['SOLR_URL_OLD']
    SOLR_NEW = app.conf['SOLR_URL_NEW']

    if bibcodelist is None:
        bibcodelist = []

    if filename is None:
        lines = []
    else:
        with open(filename) as f:
            lines = f.readlines()
        lines = map(str.strip, lines)

    bibcodes = bibcodelist + lines

    t1 = time.time()

    for bibcode in bibcodes:

        r1 = query_solr(validation_logger, SOLR_OLD[0], 'bibcode:' + bibcode, sort="bibcode desc", fl='*')
        r2 = query_solr(validation_logger, SOLR_NEW[0], 'bibcode:' + bibcode, sort="bibcode desc", fl='*')

        if (r1['response']['docs'] == []) & (r2['response']['docs'] == []):
            validation_logger.error('Bibcode {} not in either SOLR'.format(bibcode))
        elif r1['response']['docs'] == []:
            validation_logger.error('Bibcode {} missing from old SOLR'.format(bibcode))
        elif r2['response']['docs'] == []:
            validation_logger.error('Bibcode {} missing from new SOLR'.format(bibcode))
        else:
            s1 = r1['response']['docs'][0]
            s2 = r2['response']['docs'][0]

            pipeline_mismatch(bibcode, s1, s2, validation_logger)

    tottime = time.time() - t1
    validation_logger.info('Time elapsed to compare {} bibcodes: {} s'.format(len(bibcodes),tottime))

def query_solr(validation_logger, endpoint, query, start=0, rows=200, sort="date desc", fl='bibcode'):
    d = {'q': query,
         'sort': sort,
         'start': start,
         'rows': rows,
         'wt': 'json',
         'indent': 'true',
         'hl': 'true',
         'hl.fl': 'abstract,ack,body',
         }
    if fl:
        d['fl'] = fl

    response = requests.get(endpoint, params=d)
    if response.status_code == 200:
        results = response.json()
        return results
    validation_logger.warn('For query {}, there was a network problem: {0}\n'.format(query,response))
    return None

def pipeline_mismatch(bibcode, s1, s2, validation_logger):
    fields = ('abstract', 'ack', 'aff', 'alternate_bibcode', 'alternate_title', 'arxiv_class', 'author',
              'author_count', 'author_facet', 'author_facet_hier', 'author_norm', 'bibgroup', 'bibgroup_facet',
              'bibstem', 'bibstem_facet', 'body', 'citation', 'citation_count', 'cite_read_boost', 'classic_factor',
              'comment', 'copyright', 'data', 'data_count', 'data_facet', 'database', 'date', 'doctype', 'doctype_facet_hier',
              'doi', 'eid', 'email', 'entry_date', 'esources', 'first_author', 'first_author_facet_hier',
              'first_author_norm', 'fulltext_mtime', 'grant', 'grant_facet_hier', 'id', 'identifier', 'indexstamp',
              'isbn', 'issn', 'issue', 'keyword','keyword_facet', 'keyword_norm', 'keyword_schema', 'lang',
              'links_data', 'metadata_mtime', 'metrics_mtime', 'nedid', 'nedtype', 'ned_object_facet_hier', 'nonbib_mtime',
              'origin', 'orcid_mtime', 'orcid', 'orcid_pub', 'orcid_user', 'orcid_other', 'page', 'page_range',
              'page_count', 'property', 'pub', 'pub_raw', 'pubdate', 'pubnote', 'read_count', 'reader', 'recid',
              'reference', 'simbad_object_facet_hier', 'simbid', 'simbtype', 'title', 'update_timestamp', 'vizier',
              'vizier_facet', 'volume', 'year')

    ignore_fields = ('id', 'indexstamp', 'fulltext_mtime', 'links_data', 'metadata_mtime', 'metrics_mtime',
                     'nonbib_mtime', 'orcid_mtime', 'recid', 'update_timestamp')

    mismatch = 0
    missing_required = 0
    missing = 0
    notins1 = 0
    notins2 = 0

    for field in fields:
        if field not in ignore_fields:
            match = fields_match(bibcode, s1, s2, field, validation_logger)
            if match != True:
                if match == False:
                    mismatch += 1
                if match == 'required new field not in bibcode':
                    missing_required += 1
                    missing += 1
                if match == 'field not in bibcode':
                    missing += 1
                if match == 'field not in s1':
                    notins1 += 1
                if match == 'field not in s2':
                    notins2 += 1

    if mismatch == 0:
        validation_logger.info('Bibcode {}: no mismatched fields'.format(bibcode))

    validation_logger.info('The following fields are ignored: {}'.format(ignore_fields))
    validation_logger.info('Mismatch stats for bibcode {}: {} mismatches, {} missing required new fields, '
                           '{} fields not in old database, {} fields not in new database, '
                           '{} fields not in either database'.format(bibcode,mismatch,missing_required,notins1,notins2,missing))


def fields_match(bibcode, s1, s2, field, validation_logger):
    new_fields = ('data_count', 'entry_date', 'esources', 'nedid', 'nedtype', 'ned_object_facet_hier', 'origin',
                  'page_count', 'page_range')

    if (field in s1) & (field in s2):
        f1 = s1[field]
        f2 = s2[field]
    elif (field not in s1) & (field not in s2):
        if field in new_fields:
            validation_logger.warn('Bibcode {}: required new field {} not present'.format(bibcode,field))
            return 'required new field not in bibcode'
        else:
            validation_logger.info('Bibcode {}: field {} not present in either database'.format(bibcode,field))
            return 'field not in bibcode'
    elif field not in s1:
        validation_logger.info('Bibcode {}: field {} not present in old database'.format(bibcode,field))
        return 'field not in s1'
    elif field not in s2:
        validation_logger.info('Bibcode {}: field {} not present in new database'.format(bibcode,field))
        return 'field not in s2'

    # for citations, sort and compare the lists
    if field == 'citation':
        if sorted(f1) != sorted(f2):
            validation_logger.warn('Bibcode {}: different numbers of citations present in each database'.format(bibcode))
            return False
        else:
            return True

    # allow citation_count to be different by up to 3
    if field == 'citation_count':
        if abs(f1 - f2) > 3:
            validation_logger.warn(
                'Bibcode {}: citation_count field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
            return False
        else:
            return True

    # allow cite_read_boost to differ by up to 10%, unless one field is 0 and the other is non-zero
    if field == 'cite_read_boost':
        if (f1 ==0.) and (f2 == 0.):
            return True
        elif (f1 == 0. and f2 != 0.) or (f1 != 0. and f2 == 0.):
            validation_logger.warn(
                'Bibcode {}: cite_read_boost field is different between databases. Old: {} New: {}'.format(bibcode, f1,
                                                                                                           f2))
            return False
        elif (abs(f1-f2)/f1) > 0.1:
            validation_logger.warn(
                'Bibcode {}: cite_read_boost field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
            return False
        else:
            return True

    # CDS has changed to SIMBAD in new pipeline; check for this. Then check the rest of the sorted list
    if field == 'data':
        if ('CDS' in f1) and ('SIMBAD' in f2):
            f1.remove('CDS')
            f2.remove('SIMBAD')
        if sorted(f1) != sorted(f2):
            validation_logger.warn(
                'Bibcode {}: data field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
            return False
        else:
            return True

    # doctype intechreport has been changed to techreport
    if (field == 'doctype') and (f1 == 'intechreport') and (f2 == 'techreport'):
        return True

    # for identifier, sort first before comparing, since the order has changed
    if field == 'identifier':
        if sorted(f1) != sorted(f2):
            validation_logger.warn('Bibcode {}: identifier field is different between databases. Old: {} New: {}'.format(bibcode,f1,f2))
            return False
        else:
            return True

    # for references, only check that the total number is the same (otherwise sorting
    # differences can confuse it)
    if field == 'reference':
        if len(f1) != len(f2):
            validation_logger.warn(
                    'Bibcode {}: different numbers of references present in each database'.format(bibcode))
            return False
        else:
            return True

    if f1 != f2:
        # check how similar strings are
        if type(f1) is unicode:
            ratio = SequenceMatcher(None, f1, f2).ratio()
            if ratio < 0.8:
                if field == 'body':
                    validation_logger.warn(
                        'Bibcode %s: unicode field %s is different between databases.', bibcode, field,)
                else:
                    validation_logger.warn('Bibcode %s: unicode field %s is different between databases. Old: %r New: %r',bibcode,field,f1,f2)
                return False
            else:
                if field == 'body':
                    validation_logger.info(
                        'Bibcode %s: unicode field %s is slightly different between databases.',
                        bibcode, field)
                else:
                    validation_logger.info('Bibcode %s: unicode field %s is slightly different between databases. Old: %r New: %r',bibcode,field,f1,f2)
        else:
            validation_logger.warn('Bibcode {}: field {} is different between databases. Old: {} New: {}'.format(bibcode,field,f1,f2))
            return False

    return True

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

    parser.add_argument('-c',
                        '--validate_solr',
                        dest='validate',
                        action='store_true',
                        help='Compares two SOLR instances for the given bibcodes or file of bibcodes')

    parser.add_argument('-n',
                        '--filename',
                        dest='filename',
                        action='store',
                        help='File containing a list of bibcodes, one per line')
    
    args = parser.parse_args()
    
    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')
    
    
    if args.kv:
        print_kvs()

    if args.diagnostics:
        diagnostics(args.bibcodes)
        
    logger.info(args)

    if args.validate:
        validation_logger = setup_logging('solrCompare', 'INFO')
        compare_solr(validation_logger,bibcodelist=args.bibcodes,filename=args.filename)

    elif args.reindex:
        update_solr = 's' in args.reindex.lower()
        update_metrics = 'm' in args.reindex.lower()
        reindex(since=args.since, batch_size=args.batch_size, force=args.force, 
                update_solr=update_solr, update_metrics=update_metrics)