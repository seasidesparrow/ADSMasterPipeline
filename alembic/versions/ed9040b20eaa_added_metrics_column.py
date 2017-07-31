"""Added metrics column

Revision ID: ed9040b20eaa
Revises: 4475ef3e98af
Create Date: 2017-07-31 17:02:58.437069

"""

# revision identifiers, used by Alembic.
revision = 'ed9040b20eaa'
down_revision = '4475ef3e98af'

from alembic import op
import sqlalchemy as sa

                               


def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('metrics', sa.Text))
            batch_op.add_column(sa.Column('metrics_updated', sa.TIMESTAMP))
    else:
        
        op.add_column('records', sa.Column('metrics', sa.Text))
        op.add_column('records', sa.Column('metrics_updated', sa.TIMESTAMP))
        


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('metrics')
            batch_op.drop_column('metrics_updated')
    else:
        op.drop_column('records', 'metrics')
        op.drop_column('records', 'metrics_updated')
        