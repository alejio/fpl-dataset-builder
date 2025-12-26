"""Add free_transfers_available to raw_my_manager

Revision ID: f574e962ca48
Revises: 6c1ab7a2360c
Create Date: 2025-12-26 18:22:20.483547

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f574e962ca48"
down_revision: str | Sequence[str] | None = "6c1ab7a2360c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add free_transfers_available column to raw_my_manager
    op.add_column("raw_my_manager", sa.Column("free_transfers_available", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove free_transfers_available column from raw_my_manager
    op.drop_column("raw_my_manager", "free_transfers_available")
