"""add_chip_used_to_raw_my_picks

Revision ID: 659a12bce834
Revises: fb034e8d2de6
Create Date: 2025-11-21 15:51:11.302202

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "659a12bce834"
down_revision: str | Sequence[str] | None = "fb034e8d2de6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add chip_used column to raw_my_picks table."""
    op.add_column(
        "raw_my_picks",
        sa.Column("chip_used", sa.String(length=20), nullable=True),
    )
    op.create_index(op.f("ix_raw_my_picks_chip_used"), "raw_my_picks", ["chip_used"], unique=False)


def downgrade() -> None:
    """Remove chip_used column from raw_my_picks table."""
    op.drop_index(op.f("ix_raw_my_picks_chip_used"), table_name="raw_my_picks")
    op.drop_column("raw_my_picks", "chip_used")
