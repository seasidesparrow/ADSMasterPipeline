
from __future__ import absolute_import, unicode_literals
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from kombu import Queue
import math


# ============================= INITIALIZATION ==================================== #

app = app_module.ADSMasterPipelineCelery('master-pipeline')
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('route-record', app.exchange, routing_key='route-record'),
    Queue('delete-documents', app.exchange, routing_key='delete-documents'),
)


# ============================= TASKS ============================================= #

@app.task(queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.

    @param msg: protobuff that contains the following fields
        - bibcode
        - origin: (str) pipeline
        - payload: (dict)
    """
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    
    if status == 'deleted':
        task_delete_documents(msg.bibcode)
    elif status == 'active':
        type = app.get_msg_type(msg)
        
        # save into a database
        record = app.update_storage(msg.bibcode, type, msg.toJSON())
        logger.debug('Saved record: %s', record)
    
        # trigger futher processing
        task_route_record.delay(record['bibcode'])
    else:
        logger.error('Received a message with unclear status: %s', msg)


@app.task(queue='route-record')
def task_route_record(bibcode, force=False, delayed=1):
    """Receives the bibcode of a document that was updated.
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

    logger.debug('Running after-update for: %s', bibcode)

    #check if we have complete record
    r = app.get_record(bibcode)

    if r is None:
        raise Exception('The bibcode {0} doesn\'t exist!'.format(bibcode))

    bib_data_updated = r.get('bib_data_updated', None)
    orcid_claims_updated = r.get('orcid_claims_updated', None)
    nonbib_data_updated = r.get('nonbib_data_updated', None)
    fulltext_updated = r.get('fulltext_updated', None)
    metrics_updated = r.get('metrics_updated', None)

    year_zero = '1972'
    processed = r.get('processed', adsputils.get_date(year_zero))
    if processed is None:
        # It was never sent to Solr
        processed = adsputils.get_date(year_zero)

    is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])

    if is_complete:
        if force is False and all([bib_data_updated and bib_data_updated < processed,
               orcid_claims_updated and orcid_claims_updated < processed,
               nonbib_data_updated and nonbib_data_updated < processed]):
            logger.debug('Nothing to do, it was already indexed/processed')
            return
        else:
            # build the record and send it to solr
            logger.debug('Updating solr')
            solr_doc = solr_updater.transform_json_record(r)
            solr_updater.update_solr(solr_doc, app.conf.get('SOLR_URLS'))
            app.update_processed_timestamp(bibcode)
    else:
        # if we have at least the bib data, index it
        if force is True and bib_data_updated:
            logger.warn('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s)' % \
                        (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated))
            # build the record and send it to solr
            solr_doc = solr_updater.transform_json_record(r)
            solr_updater.update_solr(solr_doc, app.conf.get('SOLR_URLS'))
            app.update_processed_timestamp(bibcode)
        else:
            
            logger.warn('{bibcode} is missing bib data, even with force=True, this cannot proceed'.format(
                            bibcode=bibcode))



@app.task(queue='delete-documents')
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



if __name__ == '__main__':
    app.start()
