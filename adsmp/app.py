
from __future__ import absolute_import, unicode_literals
from . import exceptions
from .models import Records, ChangeLog, IdentifierMapping
from adsmsg import OrcidClaims, DenormalizedRecord, FulltextUpdate, MetricsRecord, NonBibRecord, NonBibRecordList, MetricsRecordList
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
        """Sends documents to solr and to Metrics DB. It will update
        the solr_processed timestamp for every document which succeeded.
        
        :param: solr_docs - list of json objects (solr documents)
        :param: solr_urls - list of strings, solr servers.
        """
        self.logger.debug('Updating solr: num_docs=%s solr_urls=%s', len(solr_docs), solr_urls)
        
        out = solr_updater.update_solr(solr_docs, solr_urls, ignore_errors=True)
        failed_bibcodes = []
        errs = [x for x in out if x != 200]
        
        if len(errs) == 0:
            self.mark_processed([x['bibcode'] for x in solr_docs], type='solr')
        else:
            self.logger.error('%s docs failed indexing', len(errs))
            # recover from erros by inserting docs one by one
            for doc in solr_docs:
                try:
                    solr_updater.update_solr([doc], solr_urls, ignore_errors=False, commit=commit)
                    self.update_processed_timestamp(doc['bibcode'], type='solr')
                    self.logger.debug('%s success', doc['bibcode'])
                except:
                    failed_bibcode = doc['bibcode']
                    self.logger.error('Failed posting data to %s\noffending payload: %s', solr_urls, doc)
                    failed_bibcodes.append(failed_bibcode)

        return failed_bibcodes

    
    def mark_processed(self, bibcodes, type=None, status = None):
        """Updates the timesstamp for all documents that match the bibcodes.
        Optionally also sets the status (which says what actually happened 
        with the document).
        
        """
        
        if type == 'solr':
            column = 'solr_processed'
        elif type == 'metrics':
            column = 'metrics_processed'
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
                    bibx = map(lambda x: x['bibcode'], batch_insert)
                    out.extend(bibx)
                    
                except exc.IntegrityError, e:
                    trans.rollback()
                    self.logger.error('Insert batch failed, will upsert one by one %s recs', len(batch_insert))
                    for x in batch_insert:
                        try:
                            self._metrics_upsert(x, session, insert_first=False)
                            out.append(x['bibcode'])
                        except Exception, e:
                            self.logger.error('Failure while updating single metrics record: %s', e)
                except Exception, e:
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
                        self.logger.warn('Tried to update=%s rows, but matched only=%s, running upsert...', 
                                         len(batch_update), r.rowcount)
                        for x in batch_update:
                            try:
                                self._metrics_upsert(x, session, insert_first=False) # do updates, db engine should not touch the same vals...
                                out.append(x['bibcode'])
                            except Exception, e:
                                self.logger.error('Failure while updating single metrics record: %s', e)
                    else:
                        bibx = map(lambda x: x['bibcode'], batch_update)
                        out.extend(bibx)
                except exc.IntegrityError, r:
                    trans.rollback()
                    self.logger.error('Update batch failed, will upsert one by one %s recs', len(batch_update))
                    for x in batch_update:
                        self._metrics_upsert(x, session, insert_first=True)
                except Exception, e:
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
            except exc.IntegrityError, e:
                trans.rollback()
                trans = session.begin_nested()
                trans.session.execute(self._metrics_table_update, record)
                trans.commit()
            except Exception, e:
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
            except exc.IntegrityError, e:
                trans.rollback()
                trans = session.begin_nested()
                trans.session.execute(self._metrics_table_insert, record)
                trans.commit()
            except Exception, e:
                trans.rollback()
                self.logger.error('DB failure: %s', e)
                raise e

    def metrics_delete_by_bibcode(self, bibcode):
        with self.metrics_session_scope() as session:
            r = session.query(MetricsRecord).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.delete(r)
                session.commit()
                return True