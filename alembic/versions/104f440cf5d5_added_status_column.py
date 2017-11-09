"""Added status column

Revision ID: 104f440cf5d5
Revises: ed9040b20eaa
Create Date: 2017-10-31 15:54:07.898609

"""

# revision identifiers, used by Alembic.
revision = '104f440cf5d5'
down_revision = 'ed9040b20eaa'

from alembic import op
import sqlalchemy as sa

                               


def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('solr_processed', sa.TIMESTAMP))
            batch_op.add_column(sa.Column('metrics_processed', sa.TIMESTAMP))
    else:
        
        op.add_column('records', sa.Column('solr_processed', sa.TIMESTAMP))
        op.add_column('records', sa.Column('metrics_processed', sa.TIMESTAMP))
        


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('metrics_processed')
            batch_op.drop_column('solr_processed')
    else:
        op.drop_column('records', 'metrics_processed')
        op.drop_column('records', 'solr_processed')
