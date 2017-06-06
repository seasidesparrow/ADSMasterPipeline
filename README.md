[![Build Status](https://travis-ci.org/adsabs/adsmp.svg)](https://travis-ci.org/adsabs/adsmp)
[![Coverage Status](https://coveralls.io/repos/adsabs/adsmp/badge.svg)](https://coveralls.io/r/adsabs/adsmp)

# adsmp

A generic template for building ADS pipeline applicaitons.

To build your own worker, first clone this repository and rename stuff.

1. git clone git@github.com:adsabs/adsmp.git
2. `init.sh ADSMyNewName`

Then commit the results into a new repository. (and remove this section from the README)       
       

## Dev Dependencies

For database/rabbitmq and others, please use: https://github.com/adsabs/devtools


## Short Summary

This pipeline is doing XYZ.


## Queues and objects

    - some-queue: it receives a silly message with a name in it and saves it into a database

## Setup (recommended)

    `$ cd adsmp/`
    `$ virtualenv python`
    `$ source python/bin/activate`
    `$ pip install -r requirements.txt`
    `$ pip install -r dev-requirements.txt`
    `$ vim local_config.py` # edit, edit
    `$ alembic upgrade head` # initialize database
    
## Testing

Always write unittests (even: always write unitests first!). Travis will run automatically. On your desktop run:

    `$ py.test`
    

## Maintainer(s)

Name, Name        