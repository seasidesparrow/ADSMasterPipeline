"""created db structure

Revision ID: 4475ef3e98af
Revises: 466986a461f1
Create Date: 2015-11-04 11:09:19.590444

"""

# revision identifiers, used by Alembic.
revision = '4475ef3e98af'
down_revision = None


from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, String, Text
                               


def upgrade():
    op.create_table('storage',
        Column('key', String(255), primary_key=True),
        Column('value', Text),
    )


def downgrade():
    op.drop_table('storage')