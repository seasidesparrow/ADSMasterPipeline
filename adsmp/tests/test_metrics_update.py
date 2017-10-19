#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import os

import unittest
from adsmp import app, models
from adsmp.models import Base, MetricsBase
from adsputils import load_config

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

unsafe = True
if os.getenv('CI') or os.getenv('TRAVIS'):
    unsafe = False

@unittest.skipIf(unsafe, 'This unittest is destructive! It will drop/recreate metrics DB schema! If you want to execute it, export CI=True first!')
class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        config = load_config()
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': config.get('METRICS_SQLALCHEMY_URL') or 'postgres://postgres@localhost:5432/metrics',
            'METRICS_SQLALCHEMY_URL': config.get('METRICS_SQLALCHEMY_URL') or 'postgres://postgres@localhost:5432/metrics',
            'SQLALCHEMY_ECHO': False,
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
            
        # records already exist - try to use wrong methods
        # inserting t2 should update it instead
        # updating test3 - should insert it instead
        t2['refereed'] = False
        app.update_metrics_db([t1, t2], [t1, test3])
        
        with app.session_scope() as session:
            b1 = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            b2 = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertFalse(b2.toJSON()['refereed'])
            b3 = session.query(models.MetricsModel).filter_by(bibcode='bib3').first()
            
            self.assertTrue(b1 and b2 and b3)
        
            
if __name__ == '__main__':
    unittest.main()
