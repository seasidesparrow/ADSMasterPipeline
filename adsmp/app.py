
from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
from . import exceptions
from .models import Records, ChangeLog, IdentifierMapping
from adsmsg import OrcidClaims, DenormalizedRecord, FulltextUpdate, MetricsRecord, NonBibRecord, NonBibRecordList, MetricsRecordList, AugmentAffiliationResponseRecord, AugmentAffiliationResponseRecordList, AugmentAffiliationRequestRecord
from adsmsg.msg import Msg
from adsputils import ADSCelery, create_engine, sessionmaker, scoped_session, contextmanager
from sqlalchemy.orm import load_only as _load_only
from sqlalchemy import Table, bindparam
import adsputils
import json
from adsmp.models import MetricsModel, MetricsBase
from adsmp import solr_updater
from adsputils import serializer
from sqlalchemy import exc
from multiprocessing.util import register_after_fork
import zlib
import requests
from copy import deepcopy
from os.path import join
import os
import sys
from kombu import BrokerConnection


class ADSMasterPipelineCelery(ADSCelery):

    def __init__(self, app_name, *args, **kwargs):
        ADSCelery.__init__(self, app_name, *args, **kwargs)
        # this is used for bulk/efficient updates to metrics db
        self._metrics_engine = self._metrics_session = None
        if self._config.get('METRICS_SQLALCHEMY_URL', None):
            self._metrics_engine = create_engine(self._config.get('METRICS_SQLALCHEMY_URL', 'sqlite:///'),
                                   echo=self._config.get('SQLALCHEMY_ECHO', False))
            _msession_factory = sessionmaker()
            self._metrics_session = scoped_session(_msession_factory)
            self._metrics_session.configure(bind=self._metrics_engine)
            
            MetricsBase.metadata.bind = self._metrics_engine
            self._metrics_table = Table('metrics', MetricsBase.metadata)
            register_after_fork(self._metrics_engine, self._metrics_engine.dispose)

            self._metrics_table_insert = self._metrics_table.insert() \
                .values({
                     'an_refereed_citations': bindparam('an_refereed_citations', required=False),
                     'an_citations': bindparam('an_citations', required=False),
                     'author_num': bindparam('author_num', required=False),
                     'bibcode': bindparam('bibcode'),
                     'citations': bindparam('citations', required=False),
                     'citation_num': bindparam('citation_num', required=False),
                     'downloads': bindparam('downloads', required=False),
                     'reads': bindparam('reads', required=False),
                     'refereed': bindparam('refereed', required=False, value=False),
                     'refereed_citations': bindparam('refereed_citations', required=False),
                     'refereed_citation_num': bindparam('refereed_citation_num', required=False),
                     'reference_num': bindparam('reference_num', required=False),
                     'rn_citations': bindparam('rn_citations', required=False),
                     'rn_citation_data': bindparam('rn_citation_data', required=False),
                    })

            self._metrics_table_update = self._metrics_table.update() \
                .where(self._metrics_table.c.bibcode == bindparam('bibcode')) \
                .values({
                     'an_refereed_citations': bindparam('an_refereed_citations', required=False),
                     'an_citations': bindparam('an_citations', required=False),
                     'author_num': bindparam('author_num', required=False),
                     'bibcode': bindparam('bibcode'),
                     'citations': bindparam('citations', required=False),
                     'citation_num': bindparam('citation_num', required=False),
                     'downloads': bindparam('downloads', required=False),
                     'reads': bindparam('reads', required=False),
                     'refereed': bindparam('refereed', required=False, value=False),
                     'refereed_citations': bindparam('refereed_citations', required=False),
                     'refereed_citation_num': bindparam('refereed_citation_num', required=False),
                     'reference_num': bindparam('reference_num', required=False),
                     'rn_citations': bindparam('rn_citations', required=False),
                     'rn_citation_data': bindparam('rn_citation_data', required=False),
                })
        # solr tweaks are read in from json files
        # when bibcodes match, the dict is used to update a solr doc
        self.tweak_dir = './tweak_files/'
        self.tweaks = {}  # key=bibcode, value=dict of solr field, value
        self.load_tweak_files()

    def load_tweak_files(self):
        """load all tweak files from the tweak directory"""
        if os.path.isdir(self.tweak_dir):
            tweak_files = os.listdir(self.tweak_dir)
            tweak_files.sort()
            for t in tweak_files:
                if t.endswith('.json'):
                    self.load_tweak_file(t)

    def load_tweak_file(self, filename):
        """load solr json tweak file"""
        self.logger.info('loading tweak file {}'.format(filename))
        try:
            with open(join(self.tweak_dir, filename)) as f:
                j = json.load(f)
                docs = j['docs']
                for doc in docs:
                    bibcode = doc.pop('bibcode')
                    if bibcode in self.tweaks:
                        self.logger.error('bibcode {} appeared in multiple tweak files'.format(bibcode))
                    self.tweaks[bibcode] = doc
        except Exception as e:
            self.logger.error('error loading tweak file {}'.format(e))

    def update_storage(self, bibcode, type, payload):
        """Update the document in the database, every time
        empty the solr/metrics processed timestamps."""
        
        if not isinstance(payload, basestring):
            payload = json.dumps(payload)

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            now = adsputils.get_date()
            oldval = None
            if type == 'metadata' or type == 'bib_data':
                oldval = r.bib_data
                r.bib_data = payload
                r.bib_data_updated = now
            elif type == 'nonbib_data':
                oldval = r.nonbib_data
                r.nonbib_data = payload
                r.nonbib_data_updated = now
            elif type == 'orcid_claims':
                oldval = r.orcid_claims
                r.orcid_claims = payload
                r.orcid_claims_updated = now
            elif type == 'fulltext':
                oldval = 'not-stored'
                r.fulltext = payload
                r.fulltext_updated = now
            elif type == 'metrics':
                oldval = 'not-stored'
                r.metrics = payload
                r.metrics_updated = now
            elif type == 'augment':
                # payload contains new value for affilation fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                r.augments = payload
                r.augments_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            session.add(ChangeLog(key=bibcode, type=type, oldvalue=oldval))
            
            r.updated = now
            out = r.toJSON()
            session.commit()
            return out


    def delete_by_bibcode(self, bibcode):
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.add(ChangeLog(key='bibcode:%s' % bibcode, type='deleted', oldvalue=serializer.dumps(r.toJSON())))
                session.delete(r)
                session.commit()
                return True
    

    def rename_bibcode(self, old_bibcode, new_bibcode):
        assert old_bibcode and new_bibcode
        assert old_bibcode != new_bibcode

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=old_bibcode).first()
            if r is not None:

                t = session.query(IdentifierMapping).filter_by(key=old_bibcode).first()
                if t is None:
                    session.add(IdentifierMapping(key=old_bibcode, target=new_bibcode))
                else:
                    while t is not None:
                        target = t.target
                        t.target = new_bibcode
                        t = session.query(IdentifierMapping).filter_by(key=target).first()


                session.add(ChangeLog(key='bibcode:%s' % new_bibcode, type='renamed', oldvalue=r.bibcode, permanent=True))
                r.bibcode = new_bibcode
                session.commit()
            else:
                self.logger.error('Rename operation, bibcode doesnt exist: old=%s, new=%s', old_bibcode, new_bibcode)


    def get_record(self, bibcode, load_only=None):
        if isinstance(bibcode, list):
            out = []
            with self.session_scope() as session:
                q = session.query(Records).filter(Records.bibcode.in_(bibcode))
                if load_only:
                    q = q.options(_load_only(*load_only))
                for r in q.all():
                    out.append(r.toJSON(load_only=load_only))
            return out
        else:
            with self.session_scope() as session:
                q = session.query(Records).filter_by(bibcode=bibcode)
                if load_only:
                    q = q.options(_load_only(*load_only))
                r = q.first()
                if r is None:
                    return None
                return r.toJSON(load_only=load_only)


    def update_processed_timestamp(self, bibcode, type=None):
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                raise Exception('Cant find bibcode {0} to update timestamp'.format(bibcode))
            if type == 'solr':
                r.solr_processed = adsputils.get_date()
            elif type == 'metrics':
                r.metrics_processed = adsputils.get_date()
            else:
                r.processed = adsputils.get_date()
            session.commit()


    def get_changelog(self, bibcode):
        out = []
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is not None:
                out.append(r.toJSON())
            to_collect = [bibcode]
            while len(to_collect):
                for x in session.query(IdentifierMapping).filter_by(key=to_collect.pop()).yield_per(100):
                    to_collect.append(x.target)
                    out.append(x.toJSON())
        return out

    def get_msg_type(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, OrcidClaims):
            return 'orcid_claims'
        elif isinstance(msg, DenormalizedRecord):
            return 'metadata'
        elif isinstance(msg, FulltextUpdate):
            return 'fulltext'
        elif isinstance(msg, NonBibRecord):
            return 'nonbib_data'
        elif isinstance(msg, NonBibRecordList):
            return 'nonbib_records'
        elif isinstance(msg, MetricsRecord):
            return 'metrics'
        elif isinstance(msg, MetricsRecordList):
            return 'metrics_records'
        elif isinstance(msg, AugmentAffiliationResponseRecord):
            return 'augment'

        else:
            raise exceptions.IgnorableException('Unkwnown type {0} submitted for update'.format(repr(msg)))


    
    def get_msg_status(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, Msg):
            status = msg.status
            if status == 1:
                return 'deleted'
            else:
                return 'active'
        else:
            return 'unknown'
        
    

    def reindex(self, solr_docs, solr_urls, commit=False):
        """Sends documents to solr. It will update
        the solr_processed timestamp for every document which succeeded.

        :param: solr_docs - list of json objects (solr documents)
        :param: solr_urls - list of strings, solr servers.
        """
        self.logger.debug('Updating solr: num_docs=%s solr_urls=%s', len(solr_docs), solr_urls)
        for doc in solr_docs:
            if doc['bibcode'] in self.tweaks:
                # apply tweaks that add/override values
                doc.update(self.tweaks[doc['bibcode']])
        # batch send solr updates
        out = solr_updater.update_solr(solr_docs, solr_urls, ignore_errors=True)
        failed_bibcodes = []
        errs = [x for x in out if x != 200]
        
        if len(errs) == 0:
            self.mark_processed([x['bibcode'] for x in solr_docs], type='solr')
        else:
            self.logger.error('%s docs failed indexing', len(errs))
            # recover from errors by inserting docs one by one
            for doc in solr_docs:
                try:
                    self.logger.error('trying individual update_solr %s', doc)
                    solr_updater.update_solr([doc], solr_urls, ignore_errors=False, commit=commit)
                    self.update_processed_timestamp(doc['bibcode'], type='solr')
                    self.logger.debug('%s success', doc['bibcode'])
                except Exception as e:
                    # if individual insert fails, 
                    # and if 'body' is in excpetion we assume Solr failed on body field
                    # then we try once more without fulltext
                    # this bibcode needs to investigated as to why fulltext/body is failing
                    failed_bibcode = doc['bibcode']                    
                    if 'body' in str(e) or 'not all arguments converted during string formatting' in str(e):
                        tmp_doc = dict(doc)
                        tmp_doc.pop('body', None)
                        try:
                            solr_updater.update_solr([tmp_doc], solr_urls, ignore_errors=False, commit=commit)
                            self.update_processed_timestamp(doc['bibcode'], type='solr')
                            self.logger.debug('%s success without body', doc['bibcode'])
                        except Exception as e:
                            self.logger.error('Failed posting bibcode %s to Solr even without fulltext\nurls: %s, offending payload %s, error is  %s', failed_bibcode, solr_urls, doc, e)
                            failed_bibcodes.append(failed_bibcode)
                    else:
                        # here if body not in error message do not retry, just note as a fail
                        self.logger.error('Failed posting individual bibcode %s to Solr\nurls: %s, offending payload %s, error is %s', failed_bibcode, solr_urls, doc, e)
                        failed_bibcodes.append(failed_bibcode)
        return failed_bibcodes

    
    def mark_processed(self, bibcodes, type=None, status = None):
        """Updates the timesstamp for all documents that match the bibcodes.
        Optionally also sets the status (which says what actually happened 
        with the document).
        
        """
        
        # avoid updating whole database (when the set is empty)
        if len(bibcodes) < 1:
            return
        
        if type == 'solr':
            column = 'solr_processed'
        elif type == 'metrics':
            column = 'metrics_processed'
        elif type == 'links':
            column = 'datalinks_processed'
        else:
            column = 'processed'
        
        now = adsputils.get_date()
        updt = {column:now}
        if status:
            updt['status'] = status
        self.logger.debug('Marking docs as processed: now=%s, num bibcodes=%s', now, len(bibcodes))
        with self.session_scope() as session:
            session.query(Records).filter(Records.bibcode.in_(bibcodes)).update(updt, synchronize_session=False)
            session.commit()

    def get_metrics(self, bibcode):
        """Helper method to retrieve data from the metrics db
        
        @param bibcode: string
        @return: JSON structure if record was found, {} else
        """
        
        if not self._metrics_session:
            raise Exception('METRCIS_SQLALCHEMU_URL not set!')
        
        with self.metrics_session_scope() as session:
            x = session.query(MetricsModel).filter(MetricsModel.bibcode == bibcode).first()
            if x:
                return x.toJSON()
            else:
                return {}


    @contextmanager
    def metrics_session_scope(self):
        """Provides a transactional session - ie. the session for the
        current thread/work of unit.

        Use as:

            with session_scope() as session:
                o = ModelObject(...)
                session.add(o)
        """

        if self._metrics_session is None:
            raise Exception('DB not initialized properly, check: METRICS_SQLALCHEMY_URL')

        # create local session (optional step)
        s = self._metrics_session()

        try:
            yield s
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            s.close()        


    def update_metrics_db(self, batch_insert, batch_update):
        """Inserts data into the metrics DB.
        :param: batch_insert - list of json objects to insert into the metrics
        :param: batch_update - list of json objects to update in metrics db
        
        :return: tupple (list-of-processed-bibcodes, exception)
        
        It tries hard to avoid raising exceptions; it will return the list
        of bibcodes that were successfully updated. It will also update
        metrics_processed timestamp for every bibcode that succeeded.
        
        """
        out = []
        
        if not self._metrics_session:
            raise Exception('You cant do this! Missing METRICS_SQLALACHEMY_URL?')
        
        self.logger.debug('Updating metrics db: len(batch_insert)=%s len(batch_upate)=%s', len(batch_insert), len(batch_update))
        
        # Note: PSQL v9.5 has UPSERT statements, the older versions don't have
        # efficient UPDATE ON DUPLICATE INSERT .... so we do it twice
        with self.metrics_session_scope() as session:
            if len(batch_insert):
                trans = session.begin_nested()
                try:
                    trans.session.execute(self._metrics_table_insert, batch_insert)
                    trans.commit()
                    bibx = [x['bibcode'] for x in batch_insert]
                    out.extend(bibx)
                    
                except exc.IntegrityError as e:
                    trans.rollback()
                    self.logger.error('Insert batch failed, will upsert one by one %s recs', len(batch_insert))
                    for x in batch_insert:
                        try:
                            self._metrics_upsert(x, session, insert_first=False)
                            out.append(x['bibcode'])
                        except Exception as e:
                            self.logger.error('Failure while updating single metrics record: %s', e)
                except Exception as e:
                    trans.rollback()
                    self.logger.error('DB failure: %s', e)
                    self.mark_processed(out, type='metrics')
                    return out, e
    
            if len(batch_update):
                trans = session.begin_nested()
                try:
                    r = session.execute(self._metrics_table_update, batch_update)
                    trans.commit()
                    if r.rowcount != len(batch_update):
                        self.logger.warning('Tried to update=%s rows, but matched only=%s, running upsert...',
                                         len(batch_update), r.rowcount)
                        for x in batch_update:
                            try:
                                self._metrics_upsert(x, session, insert_first=False) # do updates, db engine should not touch the same vals...
                                out.append(x['bibcode'])
                            except Exception as e:
                                self.logger.error('Failure while updating single metrics record: %s', e)
                    else:
                        bibx = [x['bibcode'] for x in batch_update]
                        out.extend(bibx)
                except exc.IntegrityError as r:
                    trans.rollback()
                    self.logger.error('Update batch failed, will upsert one by one %s recs', len(batch_update))
                    for x in batch_update:
                        self._metrics_upsert(x, session, insert_first=True)
                except Exception as e:
                    trans.rollback()
                    self.logger.error('DB failure: %s', e)
                    self.mark_processed(out, type='metrics')
                    return out, e
            
        self.mark_processed(out, type='metrics')
        return out, None


    def _metrics_upsert(self, record, session, insert_first=True):
        if insert_first:
            trans = session.begin_nested()
            try:
                trans.session.execute(self._metrics_table_insert, record)
                trans.commit()
            except exc.IntegrityError as e:
                trans.rollback()
                trans = session.begin_nested()
                trans.session.execute(self._metrics_table_update, record)
                trans.commit()
            except Exception as e:
                trans.rollback()
                self.logger.error('DB failure: %s', e)
                raise e
        else:
            trans = session.begin_nested()
            try:
                r = trans.session.execute(self._metrics_table_update, record)
                if r.rowcount == 0:
                    trans.session.execute(self._metrics_table_insert, record)
                trans.commit()
            except exc.IntegrityError as e:
                trans.rollback()
                trans = session.begin_nested()
                trans.session.execute(self._metrics_table_insert, record)
                trans.commit()
            except Exception as e:
                trans.rollback()
                self.logger.error('DB failure: %s', e)
                raise e

    def metrics_delete_by_bibcode(self, bibcode):
        with self.metrics_session_scope() as session:
            r = session.query(MetricsModel).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.delete(r)
                session.commit()
                return True
            
    def checksum(self, data, ignore_keys=('mtime', 'ctime', 'update_timestamp')):
        """
        Compute checksum of the passed in data. Preferred situation is when you
        give us a dictionary. We can clean it up, remove the 'ignore_keys' and
        sort the keys. Then compute CRC on the string version. You can also pass
        a string, in which case we simple return the checksum.
        
        @param data: string or dict
        @param ignore_keys: list of patterns, if they are found (anywhere) in 
            the key name, we'll ignore this key-value pair
        @return: checksum
        """
        assert isinstance(ignore_keys, tuple)

        if isinstance(data, basestring):
            if sys.version_info > (3,):
                data_str = data.encode('utf-8')
            else:
                data_str = unicode(data)
            return hex(zlib.crc32(data_str) & 0xffffffff)
        else:
            data = deepcopy(data)
            # remove all the modification timestamps
            for k, v in list(data.items()):
                for x in ignore_keys:
                    if x in k:
                        del data[k]
                        break
            if sys.version_info > (3,):
                data_str = json.dumps(data, sort_keys=True).encode('utf-8')
            else:
                data_str = json.dumps(data, sort_keys=True)
            return hex(zlib.crc32(data_str) & 0xffffffff)
    
    
    def update_remote_targets(self, solr=None, metrics=None, links=None,
                              commit_solr=False, solr_urls=None):
        """Updates remote databases/solr
            @param batch: list solr documents (already formatted)
            @param metrics: tuple with two lists, the first is a list
                of metric dicts to be inserted; the second is a list
                of metric dicts to be updated
            @param links_data: list of dicts to send to the remote
                resolver links service
            @return: 
                (set of processed bibcodes, set of bibcodes that failed)
                
            @warning: this call has a side-effect, we are emptying the passed
                in lists of data (to free up memory)
        """
        
        
        if metrics and not (isinstance(metrics, tuple) and len(metrics) == 2):
            raise Exception('Wrong data type passed in for metrics')
        
        batch = solr or []
        batch_insert, batch_update = metrics[0], metrics[1]
        links_data = links or []
        

        links_url = self.conf.get('LINKS_RESOLVER_UPDATE_URL')
        api_token = self.conf.get('ADS_API_TOKEN', '')
        recs_to_process = set()
        failed_bibcodes = []
        crcs = {}
        
        # take note of all the recs that should be updated
        # NOTE: we are assuming that set(solr) == set(metrics)_== set(links)
        for x in (batch, batch_insert, batch_update, links_data):
            if x:
                for y in x:
                    if 'bibcode' in y:
                        recs_to_process.add(y['bibcode'])
                    else:
                        raise Exception('Every record must contain bibcode! Offender: %s' % y)

        def update_crc(type, data, faileds):
            while len(data): # we are freeing memory as well
                x = data.pop()
                b = x['bibcode']
                if b in faileds:
                    continue
                crcs.setdefault(b, {})
                crcs[b][type + '_checksum'] = self.checksum(x)
                
        if len(batch):
            failed_bibcodes = self.reindex(batch, solr_urls or self.conf.get('SOLR_URLS'), commit=commit_solr)
            failed_bibcodes = set(failed_bibcodes)
            update_crc('solr', batch, failed_bibcodes)
        
        if failed_bibcodes and len(failed_bibcodes):
            self.logger.warning('SOLR failed to update some bibcodes: %s', failed_bibcodes)
            
            # when solr_urls > 1, some of the servers may have successfully indexed
            # but here we are refusing to pass data to metrics db; this seems the 
            # right choice because there is only one metrics db (but if we had many,
            # then we could differentiate) 
                    
            batch_insert = [x for x in batch_insert if x['bibcode'] not in failed_bibcodes]
            batch_update = [x for x in batch_update if x['bibcode'] not in failed_bibcodes]
            links_data = [x for x in links_data if x['bibcode'] not in failed_bibcodes]
            
            recs_to_process = recs_to_process - failed_bibcodes
            if len(failed_bibcodes):
                self.mark_processed(failed_bibcodes, type=None, status='solr-failed')
        
        metrics_exception = None
        if len(batch_insert) or len(batch_update):
            metrics_done, metrics_exception = self.update_metrics_db(batch_insert, batch_update)
            
            metrics_failed = recs_to_process - set(metrics_done)
            if len(metrics_failed):
                self.mark_processed(metrics_failed, type=None, status='metrics-failed')
            update_crc('metrics', batch_insert, metrics_failed)
            update_crc('metrics', batch_update, metrics_failed)
            
        
        if len(links_data):
            r = requests.put(links_url, data=json.dumps(links_data), headers={'Authorization': 'Bearer {}'.format(api_token)})
            if r.status_code == 200:
                self.logger.info('sent %s datalinks to %s including %s', len(links_data), links_url, links_data[0])
                update_crc('datalinks', links_data, set())
            else:
                self.logger.error('error sending links to %s, error = %s', links_url, r.text)
                self.mark_processed([x['bibcode'] for x in links_data], type=None, status='links-failed')
                recs_to_process = recs_to_process - set([x['bibcode'] for x in links_data])
            
        self.mark_processed(recs_to_process, type=None, status='success')
        if len(crcs):
            self._update_checksums(crcs)
    
        if metrics_exception:
            raise metrics_exception # will trigger retry
        if len(recs_to_process) == 0:
            raise Exception("Miserable, me, complete failure")    

    def _update_checksums(self, crcs):
        """Somewhat brain-damaged way of updating checksums
        one-by-one for each bibcode we have; but at least
        it touches all checksums for a rec in one go.
        """
        with self.session_scope() as session:
            for bibcode, vals in crcs.items():
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    raise Exception('whaay?! Cannot update crc, bibcode does not exist for: %s', bibcode)
                for k, crc in vals.items():
                    setattr(r, k, crc)
            session.commit()

    def request_aff_augment(self, bibcode, data=None):
        """send aff data for bibcode to augment affiliation pipeline

        set data parameter to provide test data"""
        if data is None:
            rec = self.get_record(bibcode)
            if rec is None:
                self.logger.warning('request_aff_augment called but no data at all for bibcode {}'.format(bibcode))
                return
            bib_data = rec.get('bib_data', None)
            if bib_data is None:
                self.logger.warning('request_aff_augment called but no bib data for bibcode {}'.format(bibcode))
                return
            aff = bib_data.get('aff', None)
            author = bib_data.get('author', '')            
            data = {
                'bibcode': bibcode,
                "aff": aff,
                "author": author,
            }
        if data and data['aff']:
            message = AugmentAffiliationRequestRecord(**data)
            self.forward_message(message)
            self.logger.info('sent augment affiliation request for bibcode {}'.format(bibcode))
        else:
            self.logger.warning('request_aff_augment called but bibcode {} has no aff data'.format(bibcode))

    def generate_links_for_resolver(self, record):
        """use nonbib or bib elements of database record and return links for resolver and checksum"""
        # nonbib data has something like
        #  "data_links_rows": [{"url": ["http://arxiv.org/abs/1902.09522"]
        # bib data has json string for:
        # "links_data": [{"access": "open", "instances": "", "title": "", "type": "preprint",

        #                 "url": "http://arxiv.org/abs/1902.09522"}]

        resolver_record = None   # default value to return
        bibcode = record.get('bibcode')
        nonbib = record.get('nonbib_data', {})
        if type(nonbib) is not dict:
            nonbib = {}    # in case database has None or something odd
        nonbib_links = nonbib.get('data_links_rows', None)
        if nonbib_links:
            # when avilable, prefer link info from nonbib
            resolver_record = {'bibcode': bibcode,
                               'data_links_rows': nonbib_links}
            
        else:
            # as a fallback, use link from bib/direct ingest
            bib = record.get('bib_data', {})
            if type(bib) is not dict:
                bib = {}
            bib_links_record = bib.get('links_data', None)
            if bib_links_record:
                try:
                    bib_links_data = json.loads(bib_links_record[0])
                    url = bib_links_data.get('url', None)
                    if url:
                        # need to change what direct sends
                        url_pdf = url.replace('/abs/', '/pdf/')
                        resolver_record = {'bibcode': bibcode,
                                           'data_links_rows': [{'url': [url],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_HTML'},
                                                               {'url': [url_pdf],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_PDF'}]}
                except (KeyError, ValueError):
                    # here if record holds unexpected value
                    self.logger.error('invalid value in bib data, bibcode = {}, type = {}, value = {}'.format(bibcode, type(bib_links_record), bib_links_record))
        return resolver_record
