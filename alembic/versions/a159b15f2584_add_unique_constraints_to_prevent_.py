"""Add unique constraints to prevent gameweek duplicates

Revision ID: a159b15f2584
Revises: 054436aec50e
Create Date: 2025-08-25 18:35:52.171490

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a159b15f2584"
down_revision: str | Sequence[str] | None = "054436aec50e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add unique constraints to prevent duplicate gameweek data."""
    # SQLite requires batch mode for adding constraints
    with op.batch_alter_table("raw_player_gameweek_performance") as batch_op:
        batch_op.create_unique_constraint("uq_player_gameweek", ["player_id", "gameweek"])

    with op.batch_alter_table("raw_my_picks") as batch_op:
        batch_op.create_unique_constraint("uq_event_player_position", ["event", "player_id", "position"])


def downgrade() -> None:
    """Remove unique constraints."""
    with op.batch_alter_table("raw_player_gameweek_performance") as batch_op:
        batch_op.drop_constraint("uq_player_gameweek", type_="unique")

    with op.batch_alter_table("raw_my_picks") as batch_op:
        batch_op.drop_constraint("uq_event_player_position", type_="unique")
