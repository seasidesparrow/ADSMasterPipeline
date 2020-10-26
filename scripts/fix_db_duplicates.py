
import argparse
import os
import sys

homedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if homedir not in sys.path:
    sys.path.append(homedir)

from adsmp import tasks
from adsmp.models import Records

# this script resolves duplicate bibcodes in the records table
# it accepts a list bibcodes to process

# for each bibcode:
#    read all rows for the bibcode
#    merge most recent data into the first record
#    update first record
#    delete duplicate rows

# the list of duplcate bibcodes can be generaed with the sql command:
#   copy (select a.bibcode from records a, records b
#     where a.bibcode=b.bibcode and a.id != b.id
#     order by a.bibcode asc) to '/tmp/masterDuplicates.txt';

# to run this code one must delete the unique constraint on bibcodes in postgres

def main():
    parser = argparse.ArgumentParser(description='Delete duplicate bibcodes from records table')
    parser.add_argument('-n',
                        '--filename',
                        dest='filename',
                        action='store',
                        help='File containing a list of bibcodes, one per line')
    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        help='List of bibcodes separated by spaces')

    args = parser.parse_args()
    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')
        for bibcode in args.bibcodes:
            process_bibcode(tasks.app, bibcode)
    elif args.filename:
        with open(args.filename, 'r') as f:
            for line in f:
                bibcode = line.strip()
                if bibcode:
                    process_bibcode(tasks.app, bibcode)
    else:
        print('error, you must supply either --bibcodes or --filename')


def process_bibcode(app, bibcode):
    with app.session_scope() as session:
        recs = session.query(Records).filter(Records.bibcode.like('%' + bibcode + '%')).all()
        if len(recs) < 2:
            print('warning: bibcode {} was not duplicated'.format(bibcode))
            return
        first = recs[0]
        for rec in recs[1:]:
            for field in ('augments', 'bib_data',
                          'fulltext', 'metrics',
                          'nonbib_data', 'orcid_claims'):
                if getattr(rec, field):
                    if getattr(first, field) is None or getattr(first, field + '_updated') < getattr(rec, field + '_updated'):
                        setattr(first, field, getattr(rec, field))
                        setattr(first, field + '_updated', getattr(rec, field + '_updated'))
        for rec in recs[1:]:
            session.delete(rec)


if __name__ == '__main__':
    main()
