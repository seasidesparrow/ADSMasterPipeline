# -*- coding: utf-8 -*-


from adsputils import get_date
from datetime import datetime
from dateutil.tz import tzutc
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, Boolean, DateTime
from sqlalchemy import types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Enum
from sqlalchemy.dialects import postgresql
import json

Base = declarative_base()
MetricsBase = declarative_base()


class UTCDateTime(types.TypeDecorator):
    impl = TIMESTAMP
    def process_bind_param(self, value, engine):
        if isinstance(value, basestring):
            return get_date(value).astimezone(tzutc())
        elif value is not None:
            return value.astimezone(tzutc()) # will raise Error is not datetime

    def process_result_value(self, value, engine):
        if value is not None:
            return datetime(value.year, value.month, value.day,
                            value.hour, value.minute, value.second,
                            value.microsecond, tzinfo=tzutc())


class KeyValue(Base):
    """Example model, it stores key/value pairs - a persistent configuration"""
    __tablename__ = 'storage'
    key = Column(String(255), primary_key=True)
    value = Column(Text)

    def toJSON(self):
        return {'key': self.key, 'value': self.value }



class Records(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19))

    bib_data = Column(Text) # 'metadata' is reserved by SQLAlchemy
    orcid_claims = Column(Text)
    nonbib_data = Column(Text)
    fulltext = Column(Text)
    metrics = Column(Text)

    bib_data_updated = Column(UTCDateTime, default=None)
    orcid_claims_updated = Column(UTCDateTime, default=None)
    nonbib_data_updated = Column(UTCDateTime, default=None)
    fulltext_updated = Column(UTCDateTime, default=None)
    metrics_updated = Column(UTCDateTime, default=None)

    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, default=get_date)
    processed = Column(UTCDateTime)

    _date_fields = ['created', 'updated', 'processed',  # dates
                      'bib_data_updated', 'orcid_claims_updated', 'nonbib_data_updated',
                      'fulltext_updated', 'metrics_updated']
    _text_fields = ['id', 'bibcode', 'fulltext']
    _json_fields = ['bib_data', 'orcid_claims', 'nonbib_data', 'metrics']

    def toJSON(self, for_solr=False, load_only=None):
        if for_solr:
            return self
        else:
            load_only = load_only and set(load_only) or set()
            doc = {}

            for f in Records._text_fields:
                if load_only and f not in load_only:
                    continue
                doc[f] = getattr(self, f, None)
            for f in Records._date_fields:
                if load_only and f not in load_only:
                    continue
                if hasattr(self, f) and getattr(self, f):
                    doc[f] = get_date(getattr(self, f))
                else:
                    doc[f] = None
            for f in Records._json_fields: # json
                if load_only and f not in load_only:
                    continue
                v = getattr(self, f, None)
                if v:
                    v = json.loads(v)
                doc[f] = v

            return doc


class ChangeLog(Base):
    __tablename__ = 'change_log'
    id = Column(Integer, primary_key=True)
    created = Column(UTCDateTime, default=get_date)
    key = Column(String(255))
    type = Column(String(255))
    oldvalue = Column(Text)
    permanent = Column(Boolean, default=False)


    def toJSON(self):
        return {'id': self.id,
                'key': self.key,
                'created': self.created and get_date(self.created).isoformat() or None,
                'oldvalue': self.oldvalue
                }


class IdentifierMapping(Base):
    """Storage for the mapping (bibcode translation) - it is a directed
    graph pointing to the most recent canonical identifier"""
    __tablename__ = 'identifiers'
    key = Column(String(255), primary_key=True)
    target = Column(String(255))

    def toJSON(self):
        return {'key': self.key, 'target': self.target }


## This definition is copied directly from: https://github.com/adsabs/metrics_service/blob/master/service/models.py
## We need to have it when we are sending/writing data into the metrics database
class MetricsModel(MetricsBase):
    __tablename__ = 'metrics'
    __bind_key__ = 'metrics'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String, nullable=False, index=True)
    refereed = Column(Boolean)
    rn_citations = Column(postgresql.REAL)
    rn_citation_data = Column(postgresql.JSON)
    downloads = Column(postgresql.ARRAY(Integer))
    reads = Column(postgresql.ARRAY(Integer))
    an_citations = Column(postgresql.REAL)
    refereed_citation_num = Column(Integer)
    citation_num = Column(Integer)
    reference_num = Column(Integer)
    citations = Column(postgresql.ARRAY(String))
    refereed_citations = Column(postgresql.ARRAY(String))
    author_num = Column(Integer)
    an_refereed_citations = Column(postgresql.REAL)
    modtime = Column(DateTime)