"""add_raw_player_gameweek_performance_table

Revision ID: 054436aec50e
Revises: eec1b36f875e
Create Date: 2025-08-25 10:53:38.292867

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "054436aec50e"
down_revision: str | Sequence[str] | None = "eec1b36f875e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add raw_player_gameweek_performance table for gameweek-by-gameweek player data."""
    op.create_table(
        "raw_player_gameweek_performance",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("player_id", sa.Integer(), nullable=False, index=True),
        sa.Column("gameweek", sa.Integer(), nullable=False, index=True),
        # Core performance stats
        sa.Column("total_points", sa.Integer(), nullable=True),
        sa.Column("minutes", sa.Integer(), nullable=True),
        sa.Column("goals_scored", sa.Integer(), nullable=True),
        sa.Column("assists", sa.Integer(), nullable=True),
        sa.Column("clean_sheets", sa.Integer(), nullable=True),
        sa.Column("goals_conceded", sa.Integer(), nullable=True),
        sa.Column("own_goals", sa.Integer(), nullable=True),
        sa.Column("penalties_saved", sa.Integer(), nullable=True),
        sa.Column("penalties_missed", sa.Integer(), nullable=True),
        sa.Column("yellow_cards", sa.Integer(), nullable=True),
        sa.Column("red_cards", sa.Integer(), nullable=True),
        sa.Column("saves", sa.Integer(), nullable=True),
        sa.Column("bonus", sa.Integer(), nullable=True),
        sa.Column("bps", sa.Integer(), nullable=True),
        # Advanced stats
        sa.Column("influence", sa.String(10), nullable=True),
        sa.Column("creativity", sa.String(10), nullable=True),
        sa.Column("threat", sa.String(10), nullable=True),
        sa.Column("ict_index", sa.String(10), nullable=True),
        # Expected stats
        sa.Column("expected_goals", sa.String(10), nullable=True),
        sa.Column("expected_assists", sa.String(10), nullable=True),
        sa.Column("expected_goal_involvements", sa.String(10), nullable=True),
        sa.Column("expected_goals_conceded", sa.String(10), nullable=True),
        # Team and context
        sa.Column("team_id", sa.Integer(), nullable=True, index=True),
        sa.Column("opponent_team", sa.Integer(), nullable=True),
        sa.Column("was_home", sa.Boolean(), nullable=True),
        # Price and selection
        sa.Column("value", sa.Integer(), nullable=True),
        sa.Column("selected", sa.Integer(), nullable=True),
        # Metadata
        sa.Column("as_of_utc", sa.DateTime(), nullable=False, index=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Remove raw_player_gameweek_performance table."""
    op.drop_table("raw_player_gameweek_performance")
