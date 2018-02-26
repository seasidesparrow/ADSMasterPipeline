"""added links

Revision ID: 991505386bc9
Revises: 104f440cf5d5
Create Date: 2018-02-14 09:42:29.869790

"""

# revision identifiers, used by Alembic.
revision = '991505386bc9'
down_revision = '104f440cf5d5'

from alembic import op
import sqlalchemy as sa

                               
# datalinks info is in nonbib data
# it is not stored as a separate column
# we just need to a column for the timestamp 
#   of when links data was processed (sent to persistent store)

def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('datalinks_processed', sa.TIMESTAMP))

    else:
        op.add_column('records', sa.Column('datalinks_processed', sa.TIMESTAMP))


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('datalinks_processed')
    else:
        op.drop_column('records', 'datalinks_processed')

