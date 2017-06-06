from .models import KeyValue, Records
from . import utils
from celery import Celery
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import json




def create_app(app_name='adsmp',
               local_config=None):
    """Builds and initializes the Celery application."""
    
    conf = utils.load_config()
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
        self._config = utils.load_config()
        self._session = None
        self._engine = None
        self.logger = utils.setup_logging(app_name, app_name) #default logger
        
    

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
        
        self.logger = utils.setup_logging(__file__, 'app', self._config.get('LOGGING_LEVEL', 'INFO'))
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
            now = utils.get_date()
            if type == 'metadata' or type == 'bib_data':
                r.bib_data = payload
                r.bib_data_updated = now 
            elif type == 'nonbib_data':
                r.nonbib_data = payload
                r.nonbib_data_updated = now
            elif type == 'orcid_claims':
                r.orcid_claims = payload
                r.orcid_claims_updated = now
            elif type == 'fulltext':
                r.fulltext = payload
                r.fulltext_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            r.updated = now
            
            session.commit()
            
