
from __future__ import absolute_import, unicode_literals
from adsmp import app as app_module
from adsmp import utils
from adsmp import exceptions
from adsmp.models import KeyValue
from celery import Task
from celery.utils.log import get_task_logger
from kombu import Exchange, Queue, BrokerConnection
import datetime


# ============================= INITIALIZATION ==================================== #

app = app_module.create_app()
exch = Exchange(app.conf.get('CELERY_DEFAULT_EXCHANGE', 'adsmp'), 
                type=app.conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))
app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-record', exch, routing_key='update-record'),
)

# set of logger used by the workers below
error_logger = utils.setup_logging('error-queue', 'task:error', app.conf.get('LOGGING_LEVEL', 'INFO'))
update_record_logger = utils.setup_logging('update-record', 'task:update_record', app.conf.get('LOGGING_LEVEL', 'INFO'))


class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        error_logger.error('{0!r} failed: {1!r}'.format(task_id, exc))



# ============================= TASKS ============================================= #

@app.task(base=MyTask, queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.
    
    @param msg: protobuff that contains the following fields
        - bibcode
        - origin: (str) pipeline
        - payload: (dict)
    """
    type = msg.origin
    if type not in ('metadata', 'orcid_claims', 'nonbib_data', 'fulltext'):
        raise exceptions.IgnorableException('Unkwnown type {0} submitted for update'.format(type))
    
    # save into a database
    app.update_storage(msg.bibcode, type, msg.payload)
    
        
        
    
    

if __name__ == '__main__':
    app.start()