#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mock import patch
from mock import mock_open
import mock
import unittest
import os
import json

import adsputils
from adsmp import app, models
from adsmp.models import Base, MetricsBase
import testing.postgresql


class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    
    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15678, user='postgres', 
                                          database='test')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        MetricsBase.metadata.bind = self.app._metrics_engine
        MetricsBase.metadata.create_all()

    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        MetricsBase.metadata.drop_all()
        self.app.close_app()

    
    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'sqlite:///'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'sqlite:///'


    def test_update_processed_timestamp(self):
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.update_processed_timestamp('abc', 'solr')
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertFalse(r.processed)
            self.assertFalse(r.metrics_processed)
            self.assertTrue(r.solr_processed)
        self.app.update_processed_timestamp('abc', 'metrics')
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertFalse(r.processed)
            self.assertTrue(r.metrics_processed)
            self.assertTrue(r.solr_processed)
        self.app.update_processed_timestamp('abc')
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertTrue(r.processed)
            self.assertTrue(r.metrics_processed)
            self.assertTrue(r.solr_processed)
        
    def test_mark_processed(self):
        self.app.mark_processed(['abc'], 'solr')
        r = self.app.get_record('abc')
        self.assertEquals(r, None)
        
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.mark_processed(['abc'], 'solr')
        r = self.app.get_record('abc')
        
        self.assertTrue(r['solr_processed'])
        self.assertFalse(r['status'])

        self.app.mark_processed(['abc'], None, status='solr-failed')
        r = self.app.get_record('abc')
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['processed'])
        self.assertEquals(r['status'], 'solr-failed')


    def test_reindex(self):
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.update_storage('foo', 'bib_data', {'bibcode': 'foo', 'hey': 1})
        
        with mock.patch('adsmp.solr_updater.update_solr', return_value=[200]):
            failed = self.app.reindex([{'bibcode': 'abc'}, 
                                       {'bibcode': 'foo'}], 
                                      ['http://solr1'])
            self.assertTrue(len(failed) == 0)
            with self.app.session_scope() as session:
                for x in ['abc', 'foo']:
                    r = session.query(models.Records).filter_by(bibcode=x).first()
                    self.assertFalse(r.processed)
                    self.assertFalse(r.metrics_processed)
                    self.assertTrue(r.solr_processed)
                    
        # pretend group failure and then success when records sent individually
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'update_processed_timestamp') as upt:
            us.side_effect = [[503], [200], [200]]
            failed = self.app.reindex([{'bibcode': 'abc'}, 
                                       {'bibcode': 'foo'}], 
                                      ['http://solr1'])
            self.assertTrue(len(failed) == 0)
            self.assertEqual(str(upt.call_args_list), "[call('abc', type=u'solr'), call('foo', type=u'solr')]")
            self.assertEqual(us.call_count, 3)
            self.assertEqual(str(us.call_args_list[-1]), "call([{'bibcode': 'foo'}], ['http://solr1'], commit=False, ignore_errors=False)") 

        # pretend failure and success without body
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'update_processed_timestamp') as upt:
            us.side_effect = [[503, 503], Exception('body failed'), 200, Exception('body failed'), 200]
            failed = self.app.reindex([{'bibcode': 'abc', 'body': 'bad body'}, 
                                       {'bibcode': 'foo', 'body': 'bad body'}], 
                                      ['http://solr1'])
            self.assertEqual(us.call_count, 5)
            self.assertTrue(len(failed) == 0)
            self.assertEqual(upt.call_count, 2)
            self.assertEqual(str(us.call_args_list[-2]), "call([{'body': 'bad body', 'bibcode': 'foo'}], ['http://solr1'], commit=False, ignore_errors=False)") 
            self.assertEqual(str(us.call_args_list[-1]), "call([{'bibcode': 'foo'}], ['http://solr1'], commit=False, ignore_errors=False)")

        # pretend failure and then lots more failure
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'update_processed_timestamp') as upt:
            us.side_effect = [[503, 503], 
                              Exception('body failed'), Exception('body failed'), 
                              Exception('body failed'), Exception('body failed')]
            failed = self.app.reindex([{'bibcode': 'abc', 'body': 'bad body'}, 
                                       {'bibcode': 'foo', 'body': 'bad body'}], 
                                      ['http://solr1'])
            self.assertEqual(us.call_count, 5)
            self.assertTrue(len(failed) == 2)
            self.assertEqual(upt.call_count, 0)

        # pretend failure and and then failure for a mix of reasons
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'update_processed_timestamp') as upt:
            us.side_effect = [[503, 503], Exception('body failed'), Exception('failed'), Exception('failed')]
            failed = self.app.reindex([{'bibcode': 'abc', 'body': 'bad body'}, 
                                       {'bibcode': 'foo', 'body': 'good body'}], 
                                      ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            self.assertTrue(len(failed) == 2)
            self.assertEqual(upt.call_count, 0)
            self.assertEqual(str(us.call_args_list[-1]), "call([{'body': 'good body', 'bibcode': 'foo'}], ['http://solr1'], commit=False, ignore_errors=False)") 

        # pretend failure and and then a mix of failure and success
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'update_processed_timestamp') as upt:
            us.side_effect = [[503, 503], Exception('body failed'), [200]]
            failed = self.app.reindex([{'bibcode': 'abc', 'body': 'bad body'}, 
                                       {'bibcode': 'foo', 'body': 'good body'}], 
                                      ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            self.assertTrue(len(failed) == 1)
            self.assertEqual(upt.call_count, 1)
            self.assertEqual(str(us.call_args_list[-1]), "call([{'body': 'good body', 'bibcode': 'foo'}], ['http://solr1'], commit=False, ignore_errors=False)") 


    def test_update_metrics(self):
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        self.app.update_storage('foo', 'metrics', {
                    'bibcode': 'foo', 
                    'citation_num': 6
                    })
        
        batch_insert = [self.app.get_record('abc')['metrics']]
        batch_update = [self.app.get_record('foo')['metrics']]
        
        bibc, errs = self.app.update_metrics_db(batch_insert, batch_update)
        self.assertEquals(bibc, ['abc', 'foo'])
        
        for x in ['abc', 'foo']:
            r = self.app.get_record(x)
            self.assertFalse(r['processed'])
            self.assertTrue(r['metrics_processed'])
            self.assertFalse(r['solr_processed'])

    def test_delete_metrics(self):
        """Makes sure we can delete a metrics record by bibcode"""
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        r = self.app.get_record('abc')
        self.app.update_metrics_db([r], [])
        m = self.app.get_metrics('abc')
        self.assertTrue(m, 'intialized metrics data')
        self.app.metrics_delete_by_bibcode('abc')
        m = self.app.get_metrics('abc')
        self.assertFalse(m, 'deleted metrics data')
        
    def test_update_records(self):
        """Makes sure we can write recs into the storage."""
        now = adsputils.get_date()
        last_time = adsputils.get_date()
        for k in ['bib_data', 'nonbib_data', 'orcid_claims']:
            self.app.update_storage('abc', k, {'foo': 'bar', 'hey': 1})
            with self.app.session_scope() as session:
                r = session.query(models.Records).filter_by(bibcode='abc').first()
                self.assertTrue(r.id == 1)
                j = r.toJSON()
                self.assertEquals(j[k], {'foo': 'bar', 'hey': 1})
                t = j[k + '_updated']
                self.assertTrue(now < t)
                self.assertTrue(last_time < j['updated'])
                last_time = j['updated']
        
        self.app.update_storage('abc', 'fulltext', {'body': 'foo bar'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertTrue(r.id == 1)
            j = r.toJSON()
            self.assertEquals(j['fulltext'], {'body': 'foo bar'})
            t = j['fulltext_updated']
            self.assertTrue(now < t)
        
        r = self.app.get_record('abc')
        self.assertEquals(r['id'], 1)
        self.assertEquals(r['processed'], None)
        
        r = self.app.get_record(['abc'])
        self.assertEquals(r[0]['id'], 1)
        self.assertEquals(r[0]['processed'], None)
        
        r = self.app.get_record('abc', load_only=['id'])
        self.assertEquals(r['id'], 1)
        self.assertFalse('processed' in r)
        
        self.app.update_processed_timestamp('abc')
        r = self.app.get_record('abc')
        self.assertTrue(r['processed'] > now)
        
        # now delete it
        self.app.delete_by_bibcode('abc')
        r = self.app.get_record('abc')
        self.assertTrue(r is None)
        with self.app.session_scope() as session:
            r = session.query(models.ChangeLog).filter_by(key='bibcode:abc').first()
            self.assertTrue(r.key, 'abc')

        # verify affilation augments can arrive in any order
        self.app.update_storage('bib_augment_test', 'augment',
                                {'affiliation': 'CfA', 'sequence': '1/2'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='bib_augment_test').first()
            j = r.toJSON()
            self.assertEquals(j['augments'], {'affiliations' : ['CfA']})
            t = j['augments_updated']
            self.assertTrue(now < t)

        self.app.update_storage('bib_augment_test', 'augment',
                                {'affiliation': 'Tufts', 'sequence': '2/2'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='bib_augment_test').first()
            j = r.toJSON()
            self.assertEquals(j['augments'], {'affiliations': ['CfA', 'Tufts']})
            t = j['augments_updated']
            self.assertTrue(now < t)

        self.app.update_storage('bib_augment_test2', 'augment',
                                {'affiliation': 'Tufts', 'sequence': '2/2'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='bib_augment_test2').first()
            j = r.toJSON()
            self.assertEquals(j['augments'], {'affiliations': ['-', 'Tufts']})
            t = j['augments_updated']
            self.assertTrue(now < t)

        self.app.update_storage('bib_augment_test2', 'augment',
                                {'affiliation': 'CfA', 'sequence': '1/2'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='bib_augment_test2').first()
            j = r.toJSON()
            self.assertEquals(j['augments'], {'affiliations': ['CfA', 'Tufts']})
            t = j['augments_updated']
            self.assertTrue(now < t)

        self.app.update_storage('bib_augment_test3', 'augment',
                                {'affiliation': 'CfA', 'sequence': '4/4'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='bib_augment_test3').first()
            j = r.toJSON()
            self.assertEquals(j['augments'], {'affiliations': ['-', '-', '-', 'CfA']})
            t = j['augments_updated']
            self.assertTrue(now < t)
            
        
    def test_rename_bibcode(self):
        self.app.update_storage('abc', 'metadata', {'foo': 'bar', 'hey': 1})
        r = self.app.get_record('abc')
        
        self.app.rename_bibcode('abc', 'def')
        
        with self.app.session_scope() as session:
            ref = session.query(models.IdentifierMapping).filter_by(key='abc').first()
            self.assertTrue(ref.target, 'def')
            
        self.assertTrue(self.app.get_changelog('abc'), [{'target': u'def', 'key': u'abc'}])

    def test_solr_tweak(self):
        """use hard coded string to verify app.tweaks is set from file"""
        test_tweak = {
            "docs": [
                {
                    "aff": [
                        "Purdue University (United States)",
                        "Purdue University (United States)",
                        "Purdue University (United States)"
                    ],
                    "aff_abbrev": [
                        "NA",
                        "NA",
                        "NA"
                    ],
                    "aff_canonical": [
                        "Not Matched",
                        "Not Matched",
                        "Not Matched"
                    ],
                    "aff_facet_hier": [],
                    "author": [
                        "Mikhail, E. M.",
                        "Kurtz, M. K.",
                        "Stevenson, W. H."
                    ],
                    "bibcode": "1971SPIE...26..187M",
                    "title": [
                        "Metric Characteristics Of Holographic Imagery"
                    ]
                }
                ]
            }
        with patch('__builtin__.open',
                   mock_open(read_data=json.dumps(test_tweak))):
            self.app.load_tweak_file('foo')
            self.assertTrue("1971SPIE...26..187M" in self.app.tweaks)
            v = test_tweak['docs'][0]
            v.pop('bibcode')
            self.assertEqual(v,
                             self.app.tweaks['1971SPIE...26..187M'])

        # verify tweaks are merged into solr record
        # reindex changed the passed dict
        with mock.patch('adsmp.solr_updater.update_solr', return_value=[200]):
            doc = {'bibcode': '1971SPIE...26..187M',
                   'abstract': 'test abstract'}
            failed = self.app.reindex([doc], ['http://solr1'])
            self.assertTrue(len(failed) == 0)
            self.assertTrue('abstract' in doc)
            self.assertTrue('aff' in doc)
            self.assertTrue('aff_abbrev' in doc)
            self.assertTrue('aff_canonical' in doc)
            self.assertTrue('aff_facet_hier' in doc)
            self.assertTrue('title' in doc)
            self.assertEqual('1971SPIE...26..187M', doc['bibcode'])

    def test_read_tweak_files(self):
        """validates code that processes directory of tweak files"""
        with mock.patch('os.path.isdir', return_value=True), \
             mock.patch('os.listdir', return_value=['foo.json', 'bar.txt']), \
             mock.patch('adsmp.app.ADSMasterPipelineCelery.load_tweak_file') as m:
            self.app.load_tweak_files()
            m.assert_called_once_with('foo.json')


if __name__ == '__main__':
    unittest.main()
