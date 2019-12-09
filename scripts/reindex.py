# purpose of this script is to rebuild a new solr collection
# it will automatically activate it (by swapping cores)

import os 
import sys
import pickle
import requests
import time
from subprocess import PIPE, Popen


homedir = os.path.dirname(os.path.dirname(__file__))
lockfile = os.path.abspath(homedir + '/reindex.lock')
solr_url = 'http://localhost:9983/solr'
admin_url = '%s/admin/cores' % solr_url
update_url = '%s/collection2/update' % solr_url

def run():
    
    # it is important that we do not run multiple times
    if os.path.exists(lockfile):
        print 'Lockfile %s already exists; exiting! (if you want to proceed, delete the file)' % (lockfile)
        data = read_lockfile(lockfile)
        for k,v in data.items():
            print '%s=%s' % (k,v)
        exit(1)
    else:
        data = {}
    
    
    try:
        # verify both cores are there
        cores = requests.get(admin_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('we dont have both cores available')
        
        assert cores['status']['collection2']['dataDir'] != cores['status']['collection1']['dataDir']
        
        print 'We are starting the indexing into collection2; once finished; we will automatically activate the new core'
        
        
        print 'First, we will delete all documents from collection2'
        r = requests.get(update_url + '?commit=true&stream.body=%3Cdelete%3E%3Cquery%3E*%3A*%3C/query%3E%3C/delete%3E&waitSearcher=true', timeout=30*60)
        r.raise_for_status()
        print 'Done deleting all docs from collection2'
        
        cores = requests.get(admin_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('we dont have both cores available')
        
        now = time.time()
        data['start'] = now
        write_lockfile(lockfile, data)
        
        command = 'python run.py --rebuild-collection collection2 >> %s/logs/reindex.log' % (homedir)
        
        retcode, stdout, stderr = execute(command, cwd=homedir)

        if retcode != 0:
            data['error'] = '%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr)
            print 'stderr=%s' % (stderr)
            raise
        
        print 'Successfully finished indexing in %s secs' % (time.time() - now)
        
        # issue commit and verify the docs are there
        r = requests.get(update_url + '?commit=true&waitSearcher=true', timeout=30*60)
        r.raise_for_status()
        
        # all went well, verify the numDocs is similar to the previous collection
        cores = requests.get(admin_url + '?wt=json').json()
        verify_collection2_size(cores['status']['collection2'])
        
        # all is well; swap the cores!
        r = requests.get(admin_url + '?action=SWAP&core=collection2&other=collection1&wt=json')
        r.raise_for_status()
        
        time.sleep(5)
        
        # verify the new core is loaded
        new_cores = requests.get(admin_url + '?wt=json').json()
        
        assert cores['status']['collection2']['dataDir'] == new_cores['status']['collection1']['dataDir']
        
        
        print 'Deleting the lock; congratulations on your new solr collection!'
        os.remove(lockfile)
        
    except Exception, e:
        print 'Failed; we will keep the process permanently locked: %s' % (e)
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
    if data.get('sizeInBytes', 0) / (1024*1024*1024.0) < 160.0: # index size at least 160GB
        raise Exception('The index is suspcisously small: %s' % (data.get('sizeInBytes', 0) / (1024*1024*1024.0)))
        
if __name__ == '__main__':
    run()