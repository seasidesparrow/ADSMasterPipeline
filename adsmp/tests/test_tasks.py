import sys
import os
import json

from mock import patch
import unittest
from adsmp import app, tasks
from adsmp.models import Base
from adsputils import get_date
from adsmsg import DenormalizedRecord, FulltextUpdate

class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), '../..')
        self._app = tasks.app
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
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
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(DenormalizedRecord(bibcode='2015ApJ...815..133S'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))


    def test_task_update_record_fulltext(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(FulltextUpdate(bibcode='2015ApJ...815..133S', body='INTRODUCTION'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))


    def test_task_update_solr(self):
        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2012'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)


    def test_task_update_solr2(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)



    def test_task_update_solr3(self):
        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)


    def test_task_update_solr4(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': None,
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
            #self.assertTrue(task_index_records.called)



    def test_task_update_solr6(self):
        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': None,
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
            self.assertFalse(task_index_records.called)

    def test_task_update_solr7(self):
        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': None,
                                                               'nonbib_data_updated': None,
                                                               'orcid_claims_updated': None,
                                                               'fulltext_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)


if __name__ == '__main__':
    unittest.main()
