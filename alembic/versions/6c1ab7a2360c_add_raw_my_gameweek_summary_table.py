"""add_raw_my_gameweek_summary_table

Revision ID: 6c1ab7a2360c
Revises: da4205aa04ae
Create Date: 2025-12-26 11:40:36.618577

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6c1ab7a2360c"
down_revision: str | Sequence[str] | None = "da4205aa04ae"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "raw_my_gameweek_summary",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event", sa.Integer(), nullable=False),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("points", sa.Integer(), nullable=True),
        sa.Column("total_points", sa.Integer(), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("overall_rank", sa.Integer(), nullable=True),
        sa.Column("bank", sa.Integer(), nullable=True),
        sa.Column("value", sa.Integer(), nullable=True),
        sa.Column("event_transfers", sa.Integer(), nullable=True),
        sa.Column("event_transfers_cost", sa.Integer(), nullable=True),
        sa.Column("points_on_bench", sa.Integer(), nullable=True),
        sa.Column("as_of_utc", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("manager_id", "event", name="uq_manager_event"),
    )
    op.create_index(
        op.f("ix_raw_my_gameweek_summary_as_of_utc"), "raw_my_gameweek_summary", ["as_of_utc"], unique=False
    )
    op.create_index(op.f("ix_raw_my_gameweek_summary_event"), "raw_my_gameweek_summary", ["event"], unique=False)
    op.create_index(
        op.f("ix_raw_my_gameweek_summary_manager_id"), "raw_my_gameweek_summary", ["manager_id"], unique=False
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_raw_my_gameweek_summary_manager_id"), table_name="raw_my_gameweek_summary")
    op.drop_index(op.f("ix_raw_my_gameweek_summary_event"), table_name="raw_my_gameweek_summary")
    op.drop_index(op.f("ix_raw_my_gameweek_summary_as_of_utc"), table_name="raw_my_gameweek_summary")
    op.drop_table("raw_my_gameweek_summary")
