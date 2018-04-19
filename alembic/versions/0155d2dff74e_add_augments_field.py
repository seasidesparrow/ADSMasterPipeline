"""add augments field

Revision ID: 0155d2dff74e
Revises: dec29f80e7d1
Create Date: 2018-04-16 13:45:25.646731

"""

# revision identifiers, used by Alembic.
revision = '0155d2dff74e'
down_revision = 'dec29f80e7d1'

from alembic import op
import sqlalchemy as sa

                               
def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('augments', sa.Text))
            batch_op.add_column(sa.Column('augments_updated', sa.TIMESTAMP))
    else:
        op.add_column('records', sa.Column('augments', sa.Text))
        op.add_column('records', sa.Column('augments_updated', sa.TIMESTAMP))


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('augments')
            batch_op.drop_column('augments_updated')
    else:
        op.drop_column('records', 'augments')
        op.drop_column('records', 'augments_updated')


