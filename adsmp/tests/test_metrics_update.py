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
from adsmp.models import Base, MetricsBase

test1 = {"refereed": True, 
     "bibcode": "bib1", 
     "downloads": [], 
     "reads": [], 
     "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
     "author_num": 1, 
     }

test2 = {"refereed": True, 
     "bibcode": "bib2", 
     "downloads": [], 
     "reads": [], 
     "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
     "author_num": 2, 
     }

test3 = {"refereed": True, 
     "bibcode": "bib3", 
     "downloads": [], 
     "reads": [], 
     "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
     "author_num": 3, 
     }

class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'postgres://master_pipeline:master_pipeline@localhost:15432/master_pipeline',
            'METRICS_SQLALCHEMY_URL': 'postgres://master_pipeline:master_pipeline@localhost:15432/master_pipeline',
            'SQLALCHEMY_ECHO': True,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        MetricsBase.metadata.bind = self.app._session.get_bind()
        MetricsBase.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        MetricsBase.metadata.drop_all()
        self.app.close_app()

    

    def test_update_records(self):
        app = self.app
        app.update_metrics_db([test1, test2], [])
        
        with app.session_scope() as session:
            r = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            self.assertTrue(r.toJSON()['refereed'])
            
            r = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertTrue(r.toJSON()['refereed'])
            
        # now update
        t1 = test1.copy()
        t2 = test2.copy()
        t1['refereed'] = False
        
        app.update_metrics_db([], [t1, t2])
        with app.session_scope() as session:
            r = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            self.assertFalse(r.toJSON()['refereed'])
            
            r = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertTrue(r.toJSON()['refereed'])
            
        # records already exist - try to insert them again (this should not fail)
        t1['refereed'] = True
        app.update_metrics_db([t2, test3], [t1, test3])
        
        with app.session_scope() as session:
            for r in session.query(models.MetricsModel).all():
                print r.toJSON()
        
            
if __name__ == '__main__':
    unittest.main()
