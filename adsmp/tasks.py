
from __future__ import absolute_import, unicode_literals
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from kombu import Queue
import math
import requests
import json
from adsmsg import MetricsRecord, NonBibRecord
from adsmsg.msg import Msg

# ============================= INITIALIZATION ==================================== #

app = app_module.ADSMasterPipelineCelery('master-pipeline')
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('index-records', app.exchange, routing_key='route-record'),
    Queue('delete-records', app.exchange, routing_key='delete-records'),
)


# ============================= TASKS ============================================= #

@app.task(queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.

    @param msg: protobuff that contains at minimum
        - bibcode
        - and specific payload
    """
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    type = app.get_msg_type(msg)
    bibcodes = []
    
    if status == 'deleted':
        if type == 'metadata':
            task_delete_documents(msg.bibcode)
        elif type == 'nonbib_records':
            for m in msg.nonbib_records: # TODO: this is very ugly, we are repeating ourselves...
                bibcodes.append(m.bibcode)
                logger.debug('Deleted %s, result: %s', type, app.update_storage(m.bibcode, 'nonbib_data', None))
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                bibcodes.append(m.bibcode)
                logger.debug('Deleted %s, result: %s', type, app.update_storage(m.bibcode, 'metrics', None))
        else:
            bibcodes.append(msg.bibcode)
            logger.debug('Deleted %s, result: %s', type, app.update_storage(msg.bibcode, type, None))
        
    elif status == 'active':
        
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                logger.debug('Saved record from list: %s', record)
        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            logger.debug('Saved record: %s', record)
    
    else:
        logger.error('Received a message with unclear status: %s', msg)



@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False):
    """
    This task is (normally) called by the cronjob task
    (that one, quite obviously, is in turn started by cron)
    
    Receives the bibcode of a document that was updated.
    (note: we could have sent the full record however we don't
    do it because the messages might be delayed and we can have
    multiple workers updating the same record; so we want to
    look into the database and get the most recent version)


    Receives bibcodes and checks the database if we have all the
    necessary pieces to push to solr. If not, then postpone and
    push later.

    We consider a record to be 'ready' if those pieces were updated
    (and were updated later than the last 'processed' timestamp):

        - bib_data
        - nonbib_data
        - orcid_claims

    'fulltext' is not considered essential; but updates to fulltext will
    trigger a solr_update (so it might happen that a document will get
    indexed twice; first with only metadata and later on incl fulltext)

    """
    
    if isinstance(bibcodes, basestring):
        bibcodes = [bibcodes]
    
    if not (update_solr or update_metrics or update_links):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running index-records for: %s', bibcodes)
    batch = []
    batch_insert = []
    batch_update = []
    links_data = []
    links_url = app.conf.get('LINKS_RESOLVER_UPDATE_URL')

    
    #check if we have complete record
    for bibcode in bibcodes:
        r = app.get_record(bibcode)
    
        if r is None:
            logger.error('The bibcode %s doesn\'t exist!', bibcode)
            continue
    
        bib_data_updated = r.get('bib_data_updated', None)
        orcid_claims_updated = r.get('orcid_claims_updated', None)
        nonbib_data_updated = r.get('nonbib_data_updated', None)
        fulltext_updated = r.get('fulltext_updated', None)
        metrics_updated = r.get('metrics_updated', None)
    
        year_zero = '1972'
        processed = r.get('processed', adsputils.get_date(year_zero))
        if processed is None:
            processed = adsputils.get_date(year_zero)
    
        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])
    
        if is_complete or (force is True and bib_data_updated):
            
            if force is False and all([
                   bib_data_updated and bib_data_updated < processed,
                   orcid_claims_updated and orcid_claims_updated < processed,
                   nonbib_data_updated and nonbib_data_updated < processed
                   ]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            
            if force:
                logger.debug('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s)' % \
                            (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated, \
                             metrics_updated))

            # build the solr record
            if update_solr:
                d = solr_updater.transform_json_record(r)
                logger.debug('Built SOLR: %s', d)
                if r.get('solr_checksum', None) != app.checksum(d):
                    batch.append(d)
                else:
                    logger.debug('Checksum identical, skipping solr update for: %s', bibcode)

            # get data for metrics
            if update_metrics:
                m = r.get('metrics', None)
                if m and r.get('metrics_checksum', None) != app.checksum(m):
                    m['bibcode'] = bibcode
                    logger.debug('Got metrics: %s', m) 
                    if r.get('processed'):
                        batch_update.append(m)
                    else:
                        batch_insert.append(m)
                else:
                    logger.debug('Checksum identical, skipping metrics update for: %s', bibcode)

            if update_links and 'nonbib_data' in r and links_url:
                nb = json.loads(r.get('nonbib_data'))
                if 'data_links_rows' in nb and r.get('links_checksum', None) != app.checksum(nb['data_links_rows']):
                    # send json version of DataLinksRow to update endpoint on links resolver
                    # need to optimize and not send one record at a time
                    tmp = {'bibcode': bibcode, 'data_links_rows': nb['data_links_rows']}
                    links_data.append(tmp)
        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s)' % \
                            (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated, \
                             metrics_updated))
    if batch or batch_insert or batch_update or links_data:
        app.update_remote_targets(solr=batch, metrics=(batch_insert, batch_update), links=links_data, commit_solr=commit)
    


@app.task(queue='delete-records')
def task_delete_documents(bibcode):
    """Delete document from SOLR and from our storage.
    @param bibcode: string
    """
    logger.debug('To delete: %s', bibcode)
    app.delete_by_bibcode(bibcode)
    deleted, failed = solr_updater.delete_by_bibcodes([bibcode], app.conf['SOLR_URLS'])
    if len(failed):
        logger.error('Failed deleting documents from solr: %s', failed)
    if len(deleted):
        logger.debug('Deleted SOLR docs: %s', deleted)

    if app.metrics_delete_by_bibcode(bibcode):
        logger.debug('Deleted metrics record: %s', bibcode)
    else:
        logger.debug('Failed to deleted metrics record: %s', bibcode)


if __name__ == '__main__':
    app.start()
