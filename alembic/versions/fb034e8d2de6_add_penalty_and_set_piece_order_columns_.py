"""Add penalty and set-piece order columns to player snapshots

Revision ID: fb034e8d2de6
Revises: a159b15f2584
Create Date: 2025-10-29 19:32:31.938180

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fb034e8d2de6"
down_revision: str | Sequence[str] | None = "a159b15f2584"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add penalty and set-piece order columns to player snapshots."""
    op.add_column("raw_player_gameweek_snapshot", sa.Column("penalties_order", sa.Integer(), nullable=True))
    op.add_column(
        "raw_player_gameweek_snapshot", sa.Column("corners_and_indirect_freekicks_order", sa.Integer(), nullable=True)
    )
    op.add_column("raw_player_gameweek_snapshot", sa.Column("direct_freekicks_order", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Remove penalty and set-piece order columns from player snapshots."""
    op.drop_column("raw_player_gameweek_snapshot", "direct_freekicks_order")
    op.drop_column("raw_player_gameweek_snapshot", "corners_and_indirect_freekicks_order")
    op.drop_column("raw_player_gameweek_snapshot", "penalties_order")
