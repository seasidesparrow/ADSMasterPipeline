# purpose of this script is to rebuild a new solr collection
# it will automatically activate it (by swapping cores)

import os 
import sys
import pickle
import requests
import time

homedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if homedir not in sys.path:
    sys.path.append(homedir)

from subprocess import PIPE, Popen
from adsmp import tasks
from adsputils import setup_logging


app = tasks.app
logger = setup_logging('rebuild')
lockfile = os.path.abspath(homedir + '/rebuild.locked')


def get_solr_url(path):
    # use what is configured in current config, but only the first instance
    urls = app.conf.get('SOLR_URLS')
    s = urls[0].split('/collection')[0]
    if path.startswith('/'):
        return s + path
    else:
        return s + '/' + path


cores_url = get_solr_url('/admin/cores')
update_url = get_solr_url('/collection2/update')


def run():
    # it is important that we do not run multiple times
    if os.path.exists(lockfile):
        sys.stderr.write('Lockfile %s already exists; exiting! (if you want to proceed, delete the file)\n' % (lockfile))
        data = read_lockfile(lockfile)
        for k,v in data.items():
            sys.stderr.write('%s=%s\n' % (k,v))
        exit(1)
    else:
        data = {}
    
    
    try:
        # verify both cores are there
        cores = requests.get(cores_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('we dont have both cores available')
        
        assert cores['status']['collection2']['dataDir'] != cores['status']['collection1']['dataDir']
        
        logger.info('We are starting the indexing into collection2; once finished; we will automatically activate the new core')
        
        
        logger.info('First, we will delete all documents from collection2')
        r = requests.get(update_url + '?commit=true&stream.body=%3Cdelete%3E%3Cquery%3E*%3A*%3C/query%3E%3C/delete%3E&waitSearcher=true', timeout=60*60)
        r.raise_for_status()
        logger.info('Done deleting all docs from collection2')
        
        cores = requests.get(cores_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('We dont have both cores available')
        
        now = time.time()
        data['start'] = now
        write_lockfile(lockfile, data)
        
        command = 'python run.py --rebuild-collection --solr-collection collection2 >> %s/logs/reindex.log' % (homedir)
        retcode, stdout, stderr = execute(command, cwd=homedir)

        if retcode != 0:
            data['error'] = '%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr)
            logger.warn('stderr=%s' % (stderr))
            raise
        
        logger.info('Successfully finished indexing in %s secs' % (time.time() - now))
        
        # issue commit and verify the docs are there
        r = requests.get(update_url + '?commit=true&waitSearcher=true', timeout=30*60)
        r.raise_for_status()
        logger.info('Issued commit to SOLR')
        
        # all went well, verify the numDocs is similar to the previous collection
        cores = requests.get(cores_url + '?wt=json').json()
        verify_collection2_size(cores['status']['collection2'])
        logger.info('Successfully verified the collection')
        
        # all is well; swap the cores!
        r = requests.get(cores_url + '?action=SWAP&core=collection2&other=collection1&wt=json')
        r.raise_for_status()
        logger.info('Swapped collection1 with collection2')
        
        logger.info('Going to sleep for few secs...')
        time.sleep(5)
        
        # verify the new core is loaded
        new_cores = requests.get(cores_url + '?wt=json').json()
        assert cores['status']['collection2']['dataDir'] == new_cores['status']['collection1']['dataDir']
        logger.info('Verified the new collection is in place')
        
        
        logger.info('Deleting the lock; congratulations on your new solr collection!')
        os.remove(lockfile)
        
    except Exception, e:
        logger.error('Failed; we will keep the process permanently locked: %s' % (e))
        sys.stderr.write('Failed. Please see logs for more details')
        data['last-exception'] = str(e)
        write_lockfile(lockfile, data)    

    

def execute(command, **kwargs):
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, **kwargs)
    out, err = p.communicate()
    return (p.returncode, out, err)


def read_lockfile(lockfile):
    with open(lockfile, 'rb') as f:
        return pickle.load(f)


def write_lockfile(lockfile, data):
    with open(lockfile, 'wb') as f:
        pickle.dump(data, f)


def verify_collection2_size(data):
    if data.get('numDocs', 0) <= 14150713:
        raise Exception('Too few documents in the new index: %s' % data.get('numDocs', 0))
    if data.get('sizeInBytes', 0) / (1024*1024*1024.0) >= 146.0: # index size at least 146GB
        raise Exception('The index is suspiciously small: %s' % (data.get('sizeInBytes', 0) / (1024*1024*1024.0)))

        
if __name__ == '__main__':
    run()
