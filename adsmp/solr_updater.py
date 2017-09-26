import requests
import json
from adsputils import setup_logging, date2solrstamp
import time


logger = setup_logging('solr_updater')

# When building SOLR record, we grab data from the database and insert them
# into the dictionary with the following conventions:

# 'destination' (string) == insert the value into record[destination]
# '' (empty value) == extend the existing values with what you find under this key
# None == ignore the value completely
# function == receives the data (and solr doc as built already), should return dict 

def get_nonbib_for_solr(data, solrdoc):
    return dict(
                citation=data.get('citations', []),
                citation_count=data.get('citation_count', 0),
                cite_read_boost=data.get('boost', 0.0),
                reference=data.get('reference', []),
                )
def get_orcid_claims(data, solrdoc):
    out = {}
    # TODO(rca): shall we check that list of authors corresponds?
    if 'verified' in data:
        out['orcid_user'] = data['verified']
    if 'unverified' in data:
        out['orcid_other'] = data['unverified']
    return out

fmap = dict(metadata_mtime='bib_data_updated',
           nonbib_mtime='nonbib_data_updated',
           fulltext_mtime='fulltext_updated',
           orcid_mtime='orcid_claims_updated',
           metrics_mtime='metrics_updated'
           )
def get_timestamps(db_record, out):
    out = {}
    last_update = None
    for k,v in fmap.items():
        if v in db_record and db_record[v]:
            t = db_record[v]
            out[k] = date2solrstamp(t)
            if last_update is None or t > last_update:
                last_update = t
    if last_update:
        out['update_timestamp'] = date2solrstamp(last_update)
    return out
     
DB_COLUMN_DESTINATIONS = {
    'bib_data': '', 
    'orcid_claims': get_orcid_claims, 
    'nonbib_data': get_nonbib_for_solr,
    'id': 'id', 
    'fulltext': 'body',
    '#timestamps': get_timestamps, # use 'id' to be always called
    
    }


def delete_by_bibcodes(bibcodes, urls):
    '''Deletes records from SOLR, it returns the databstructure with 
    indicating which bibcodes were deleted.'''
  
    deleted = []
    failed = []
    headers = {"Content-Type":"application/json"}
    for bibcode in bibcodes:
        logger.info("Delete: %s" % bibcode)
        data = json.dumps({'delete':{"query":'bibcode:"%s"' % bibcode}})
        i = 0
        for url in urls:
            r = requests.post(url, headers=headers, data=data)
            if r.status_code == 200:
                i += 1
        if i == len(urls):
            deleted.append(bibcode)
        else:
            failed.append(bibcode)
    return (deleted, failed)
        


def update_solr(json_records, solr_urls, ignore_errors=False):
    """ Sends data to solr
        :param: json_records - list of JSON formatted data (formatted in the way
                that SOLR expects)
        :param: solr_urls: list of urls where to post data to
        :param: ignore_errors: (True) if to generate an exception if a status 
                code as returned from SOLR is not 200
        :return:  
    """
    if not isinstance(json_records, list):
        json_records = [json_records]
    payload = json.dumps(json_records)
    out = []
    for url in solr_urls:
        r = requests.post(url, data=payload, headers={'content-type': 'application/json'})
        if r.status_code != 200:
            logger.error("Error sending data to solr\nurl=%s\nresponse=%s\ndata=%s", url, payload, r.text)
            if ignore_errors == True:
                out.append(r.status_code)
            else:
                raise Exception('Error posting data to SOLR: %s (err code: %s)' % (url, r.status_code))
    return out
            
    

def transform_json_record(db_record):
    out = {'bibcode': db_record['bibcode']}
    
    # order timestamps (if any)
    timestamps = []
    for k, v in DB_COLUMN_DESTINATIONS.items():
        ts = db_record.get(k + '_updated', None)
        if ts:
            ts = time.mktime(ts.timetuple())
        else:
            ts = -1  
        timestamps.append((k, v, ts))
    timestamps.sort(key=lambda x: x[2])
    
    for field, target, _ in timestamps:
        if db_record.get(field, None):
            if target:
                if callable(target):
                    x = target(db_record.get(field), out) # in the interest of speed, don't create copy of out
                    if x:
                        out.update(x) 
                else:
                    out[target] = db_record.get(field)
            else:
                if target is None:
                    continue
                out.update(db_record.get(field))
        elif field.startswith('#'):
            if callable(target):
                x = target(db_record, out) # in the interest of speed, don't create copy of out
                if x:
                    out.update(x) 
    
    return out




