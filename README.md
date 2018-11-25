[![Build Status](https://travis-ci.org/adsabs/ADSMasterPipeline.svg)](https://travis-ci.org/adsabs/ADSMasterPipeline)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSMasterPipeline/badge.svg)](https://coveralls.io/r/adsabs/ADSMasterPipeline)

# adsmp

## Short Summary

This pipeline is collecting results from the sub-ordinate pipelines (bibliographic, non-bibliographic, fulltext, orcid claims, metrics). It also updates SOLR, Metrics DB and link resolver.


## Queues and objects

    - update-record: input from the 'other' pipelines is collected here
    - index-record: internal queue, it forwards data to solr/metrics
    - delete-record: removes from solr/metrics db

## Setup (recommended)

    `$ virtualenv python`
    `$ source python/bin/activate`
    `$ pip install -r requirements.txt`
    `$ pip install -r dev-requirements.txt`
    `$ vim local_config.py` # edit, edit
    `$ alembic upgrade head` # initialize database

## Important note

The pipeline will NOT send anything to SOLR/Metrics DB by default. You should trigger the update using a cronjob. There are two important modes:

    - normal mode (`python run.py -r`): will discover all updates that happened since the last invocation
        of the normal mode and will send them to the `index-records` queue; the parameter force will be set to False; hence only documents that have both metadata, orcid claims, and non-bib data will get sent to solr
        
    - pushy mode (`python run.py -r -f`) will discover all updates since the last invocation of the 'pushy' mode; and will send them to `index-records` queue and set force=True; this will force the worker to submit data to solr immediately (so effectively, this means any update to a record triggers push). Regardless, we always wait till we have bibliographic metadata.
        
 It is **imperative** that both modes of operation be used together in the 24h cycle. The normal mode will ignore some (many)
 records - so you must call the `pushy` mode at least at the end of the quiet period. The suggested schedule is the following (all times UTC):
 
  00:00-05:00 | normal mode, invoked every 5 mins
  05:00-06:00 | catch updates, invoke forced mode
  06:01-17:00 | --- do nothing, solr replication is not happening
  17:01-22:00 | normal mode, invoked every 5 mins
  22:01-23:59 | forced mode, catch updates 


## Solr Tweaks

Sometimes we want to modify Solr records for specific bibcodes.  The most common use case is when we add a new Solr field.  Previously, we developed and deployed pipeline code to compute the new field, passed this data to master and extended master to send the new field to Solr.  For very complex fields, this takes too long.  It delays the prototyping of UI code until all pipeline changes are in production.  Solr tweaks allows deployment of test values for a small number of bibcodes to Solr.  Simply put the values in a properly formatted .json file in the /app/tweak_files directory and restart the workers.  Workers must be restarted because the read all the tweak files during initialization.  Solr tweak files contain:
```
{
    "docs": [
        {
            "bibcode": "1971SPIE...26..187M",
            "aff": [
                "Purdue University (United States)",
                "Purdue University (United States)",
                "Purdue University (United States)"
            ],
            "aff_abbrev": [
                "NA",
                "NA",
                "NA"
            ],
            "aff_canonical": [
                "Not Matched",
                "Not Matched",
                "Not Matched"
            ],
            "aff_facet_hier": [],
            "author": [
                "Mikhail, E. M.",
                "Kurtz, M. K.",
                "Stevenson, W. H."
            ],
            "title": [
                "Metric Characteristics Of Holographic Imagery"
            ]
        },
	...
```
Just before master sends a Solr doc to Solr, master checks if there is a tweak for the current bibcode.  If so, the Solr doc dict is updated with the tweak dict.  New key/value pairs are added and, where the key already exists in the Solr doc dict, its value is changed to match the value in the tweak dict.

Solr tweaks do not change any values in master's Records table.  To remove the tweaks, delete the tweak file(s), restart the celery workers and reindex the tweaked bibcodes.  A bibcode should not be repeated either in a single tweak file or in multiple tweak files.  When multiple tweaks for a given bibcode are provided only one tweak is applied and an error is logged.

## Testing

Always write unittests (even: always write unitests first!). Travis will run automatically. On your desktop run:

    `$ py.test`
    

## Maintainer(s)

Roman, Sergi, Steve
