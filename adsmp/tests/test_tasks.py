import sys
import os
import json

from mock import patch
import unittest
from adsmp import app, utils, tasks
from adsmp.models import Base


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


    def test_task_update_record(self, next_task, update_storage, *args):
        with patch(tasks.task_after_record_update, 'delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(RecordMessage(bibcode='2015ApJ...815..133S', 
                                                   update_type='metadata', 
                                                   payload={'bibcode': '2015ApJ...815..133S', 'title': 'foo bar'})
            self.assertTrue(next_task.called)
            rec = next_task.call_args[0][1].payload
            print rec # test it is the full record
            


            
            

if __name__ == '__main__':
    unittest.main()