import sys
import os
import json

from mock import patch
import unittest
from adsmp import app, utils, tasks
from adsmp.models import Base
from adsmp.utils import get_date

class TestWorkers(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), '../..')
        self._app = tasks.app
        self.app = app.create_app('test',
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False
            })
        tasks.app = self.app # monkey-patch the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app


    def test_task_update_record(self):
        with patch('adsmp.tasks.task_route_record.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(BibRecord(bibcode='2015ApJ...815..133S', 
                                                   title='foo bar'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))

            

    
    def test_task_update_solr(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2012'),}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S')
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
            

    def test_task_update_solr2(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
        

    
    def test_task_update_solr3(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)

            
    def test_task_update_solr4(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': None,
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
            self.assertTrue(task_route_record.called)
    
        
    def test_task_update_solr5(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': None,
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S', force=True, delayed=2)
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
            self.assertTrue(task_route_record.called)
            task_route_record.assert_called_with(('2015ApJ...815..133S', 3), countdown=100.0)
    
    
    def test_task_update_solr6(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': None,
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_route_record.apply_async', return_value=None) as task_route_record:
            
            self.assertFalse(update_solr.called)
            tasks.task_route_record('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
            self.assertFalse(task_route_record.called)            


if __name__ == '__main__':
    unittest.main()