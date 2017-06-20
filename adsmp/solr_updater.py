import requests
import json
from adsputils import setup_logging


logger = setup_logging('solr_updater')

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
        


def update_solr(json_records, solr_urls):
    if not isinstance(json_records, list):
        json_records = [json_records]
    payload = json.dumps(json_records)
    for url in solr_urls:
        r = requests.post(url, data=payload, headers={'content-type': 'application/json'})
    

def transform_json_record(db_record):
    out = {'bibcode': db_record['bibcode']}
    
    for f, t in [('id', 'id'), ('bib_data', ''), ('nonbib_data', 'adsdata'), ('orcid_claims', 'orcid_claims')]:
        if db_record.get(f, None):
            if t:
                out[t] = db_record.get(f)
            else:
                out.update(db_record.get(f))
    if db_record.get('fulltext', None):
        out['body'] = db_record['fulltext']
    
    return out



