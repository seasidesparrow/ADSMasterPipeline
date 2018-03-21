"""Added checksums

Revision ID: dec29f80e7d1
Revises: 991505386bc9
Create Date: 2018-03-15 14:36:50.242679

"""

# revision identifiers, used by Alembic.
revision = 'dec29f80e7d1'
down_revision = '991505386bc9'

from alembic import op
import sqlalchemy as sa        


def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('solr_checksum', sa.String(10)))
            batch_op.add_column(sa.Column('metrics_checksum', sa.String(10)))
            batch_op.add_column(sa.Column('datalinks_checksum', sa.String(10)))
    else:
        
        op.add_column('records', sa.Column('solr_checksum', sa.String(10)))
        op.add_column('records', sa.Column('metrics_checksum', sa.String(10)))
        op.add_column('records', sa.Column('datalinks_checksum', sa.String(10)))


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('solr_checksum')
            batch_op.drop_column('metrics_checksum')
            batch_op.drop_column('datalinks_checksum')
    else:
        op.drop_column('records', 'solr_checksum')
        op.drop_column('records', 'metrics_checksum')
        op.drop_column('records', 'datalinks_checksum')
