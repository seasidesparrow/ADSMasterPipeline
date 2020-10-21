
import unittest
import os
import testing.postgresql
import time

import adsputils
from adsmp import app, models
from adsmp.models import Base
from scripts import fix_db_duplicates


@unittest.skip("only works with old Record table definition without bibcode unique=True")
class TestFixDbDuplicates(unittest.TestCase):
    
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

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_duplicates(self):
        with self.app.session_scope() as session:
            session.add(models.Records(bibcode='abc',
                                       bib_data="{'bibcode': 'abc', 'hello': 1}",
                                       bib_data_updated=adsputils.get_date(),
                                       nonbib_data="{'bibcode': 'abc', 'world': 1}",
                                       nonbib_data_updated=adsputils.get_date()))
            time.sleep(.1)
            session.add(models.Records(bibcode='abc',
                                       bib_data="{'bibcode': 'abc', 'hello': 2}",
                                       bib_data_updated=adsputils.get_date(),
                                       nonbib_data="{'bibcode': 'abc', 'world': 2}",
                                       nonbib_data_updated=adsputils.get_date()))
            session.commit()
            time.sleep(.1)
            session.add(models.Records(bibcode='abc',
                                       bib_data="{'bibcode': 'abc', 'hello': 3}",
                                       bib_data_updated=adsputils.get_date()))
            recs = session.query(models.Records).filter(models.Records.bibcode.like('%abc%')).all()
            self.assertEqual(3, len(recs))

        fix_db_duplicates.process_bibcode(self.app, 'abc')
        with self.app.session_scope() as session:
            recs = session.query(models.Records).filter(models.Records.bibcode.like('%abc%')).all()
            self.assertEqual(1, len(recs))
            r = recs[0]
            self.assertEqual("{'bibcode': 'abc', 'hello': 3}", r.bib_data)
            self.assertEqual("{'bibcode': 'abc', 'world': 2}", r.nonbib_data)

    def test_duplicates_with_none(self):
        with self.app.session_scope() as session:
            session.add(models.Records(bibcode='abc',
                                       bib_data="{'bibcode': 'abc', 'hello': 1}",
                                       bib_data_updated=adsputils.get_date()))
            time.sleep(.1)
            session.add(models.Records(bibcode='abc',
                                       nonbib_data="{'bibcode': 'abc', 'world': 2}",
                                       nonbib_data_updated=adsputils.get_date()))
            session.commit()
            recs = session.query(models.Records).filter(models.Records.bibcode.like('%abc%')).all()
            self.assertEqual(2, len(recs))

        fix_db_duplicates.process_bibcode(self.app, 'abc')
        with self.app.session_scope() as session:
            recs = session.query(models.Records).filter(models.Records.bibcode.like('%abc%')).all()
            self.assertEqual(1, len(recs))
            r = recs[0]
            self.assertEqual("{'bibcode': 'abc', 'hello': 1}", r.bib_data)
            self.assertEqual("{'bibcode': 'abc', 'world': 2}", r.nonbib_data)


if __name__ == '__main__':
    unittest.main()
