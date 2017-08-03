#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import os

import unittest
import json
import re
import os
import math
import mock
import adsputils
from mock import patch
from io import BytesIO
from datetime import datetime
from adsmp import app, models
from adsmp.models import Base

class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    
    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'sqlite:///'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'sqlite:///'

    

    
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
        
        self.app.update_storage('abc', 'fulltext', 'foo bar')
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertTrue(r.id == 1)
            j = r.toJSON()
            self.assertEquals(j['fulltext'], u'foo bar')
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
        
    def test_rename_bibcode(self):
        self.app.update_storage('abc', 'metadata', {'foo': 'bar', 'hey': 1})
        r = self.app.get_record('abc')
        
        self.app.rename_bibcode('abc', 'def')
        
        with self.app.session_scope() as session:
            ref = session.query(models.IdentifierMapping).filter_by(key='abc').first()
            self.assertTrue(ref.target, 'def')
            
        self.assertTrue(self.app.get_changelog('abc'), [{'target': u'def', 'key': u'abc'}])
    
if __name__ == '__main__':
    unittest.main()
