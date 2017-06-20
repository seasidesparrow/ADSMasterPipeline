
from __future__ import absolute_import, unicode_literals
import adsputils
from . import exceptions
from .models import KeyValue, Records, ChangeLog, IdentifierMapping
from celery import Celery
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import load_only as _load_only
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import collections
import copy
import json
from adsmsg import OrcidClaims, BibRecord



def create_app(app_name='adsmp',
               local_config=None):
    """Builds and initializes the Celery application."""
    
    conf = adsputils.load_config()
    if local_config:
        conf.update(local_config)

    app = ADSMasterPipelineCelery(app_name,
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=conf.get('CELERY_INCLUDE', ['adsmp.tasks']))

    app.init_app(conf)
    return app



class ADSMasterPipelineCelery(Celery):
    
    def __init__(self, app_name, *args, **kwargs):
        Celery.__init__(self, *args, **kwargs)
        self._config = adsputils.load_config()
        self._session = None
        self._engine = None
        self.logger = adsputils.setup_logging(app_name) #default logger
        
    

    def init_app(self, config=None):
        """This function must be called before you start working with the application
        (or worker, script etc)
        
        :return None
        """
        
        if self._session is not None: # the app was already instantiated
            return
        
        if config:
            self._config.update(config) #our config
            self.conf.update(config) #celery's config (devs should be careful to avoid clashes)
        
        self.logger = adsputils.setup_logging('app', self._config.get('LOGGING_LEVEL', 'INFO'))
        self._engine = create_engine(config.get('SQLALCHEMY_URL', 'sqlite:///'),
                               echo=config.get('SQLALCHEMY_ECHO', False))
        self._session_factory = sessionmaker()
        self._session = scoped_session(self._session_factory)
        self._session.configure(bind=self._engine)
    
    
    def close_app(self):
        """Closes the app"""
        self._session = self._engine = self._session_factory = None
        self.logger = None
    
        
    @contextmanager
    def session_scope(self):
        """Provides a transactional session - ie. the session for the 
        current thread/work of unit.
        
        Use as:
        
            with session_scope() as session:
                o = ModelObject(...)
                session.add(o)
        """
    
        if self._session is None:
            raise Exception('init_app() must be called before you can use the session')
        
        # create local session (optional step)
        s = self._session()
        
        try:
            yield s
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            s.close()
            
    
    def update_storage(self, bibcode, type, payload):
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
                oldval = r.fulltext
                r.fulltext = payload
                r.fulltext_updated = now
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
                session.add(ChangeLog(key=bibcode, type='deleted', oldvalue=r.toJSON()))
                session.delete(r)
                session.commit()
    
    
    def rename_bibcode(self, old_bibcode, new_bibcode):
        assert old_bibcode and new_bibcode
        assert old_bibcode != new_bibcode
        
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=old_bibcode).first()
            if r is not None:
                
                t = session.query(IdentifierMapping).filter_by(bibcode=old_bibcode).first()
                if t is None:
                    session.add(IdentifierMapping(key=old_bibcode, target=new_bibcode))
                else:
                    while t is not None:
                        target = t.target
                        t.target = new_bibcode
                        t = session.query(IdentifierMapping).filter_by(bibcode=target).first()
                
                
                session.add(ChangeLog(key=new_bibcode, type='renamed', oldvalue=r.bibcode, permanent=True))
                r.bibcode = new_bibcode
                session.commit()
    
    
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
       
    
    def update_processed_timestamp(self, bibcode):
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                raise Exception('Cant find bibcode {0} to update timestamp'.format(bibcode))
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
                for x in session.query(IdentifierMapping).filter_by(key=to_collect.pop()).all():
                    if x.type == 'renamed':
                        to_collect.append(x.oldvalue)
                    out.append(x.toJSON())
        return out
    
    def get_msg_type(self, msg):
        """Identifies the type of this supplied message.
        
        :param: Protobuf instance
        :return: str
        """
        
        if isinstance(msg, OrcidClaims):
            return 'orcid_claims'
        elif isinstance(msg, BibRecord):
            return 'metadata'
        else:
            raise exceptions.IgnorableException('Unkwnown type {0} submitted for update'.format(repr(msg)))
    
                
            
