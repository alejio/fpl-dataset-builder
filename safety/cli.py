"""CLI interface for data safety operations."""

import typer

from .backup import create_safety_backup, data_safety
from .integrity import validate_data_integrity


def create_safety_cli() -> typer.Typer:
    """Create CLI for data safety operations."""
    app = typer.Typer(help="Data Safety Management for FPL Dataset Builder")

    @app.command()
    def backup(suffix: str = typer.Option("manual_backup", help="Backup suffix")):
        """Create a full backup of all critical data files."""
        backups = create_safety_backup(suffix)
        typer.echo(f"✅ Created backup of {len(backups)} files")
        for filename, backup_path in backups.items():
            typer.echo(f"  {filename} -> {backup_path.name}")

    @app.command()
    def validate():
        """Validate data consistency across all datasets."""
        results = validate_data_integrity()
        typer.echo("🔍 Data Consistency Validation:")
        for check, passed in results.items():
            status = "✅" if passed else "❌"
            typer.echo(f"  {status} {check}")

    @app.command()
    def summary():
        """Show summary of all datasets."""
        summary = data_safety.get_data_summary()
        typer.echo("📊 Dataset Summary:")
        for filename, info in summary.items():
            if 'error' in info:
                typer.echo(f"  ❌ {filename}: {info['error']}")
            elif 'status' in info:
                typer.echo(f"  ⚠️  {filename}: {info['status']}")
            else:
                typer.echo(f"  ✅ {filename}: {info['rows']:,} rows, {info['size_mb']} MB")

    @app.command()
    def restore(filename: str, timestamp: str = typer.Option(None, help="Backup timestamp")):
        """Restore a file from backup."""
        success = data_safety.emergency_restore(filename, timestamp)
        if success:
            typer.echo(f"✅ Successfully restored {filename}")
        else:
            typer.echo(f"❌ Failed to restore {filename}")

    @app.command()
    def cleanup(days: int = typer.Option(7, help="Keep backups for this many days")):
        """Clean up old backup files."""
        data_safety.cleanup_old_backups(days)
        typer.echo(f"✅ Cleaned up backups older than {days} days")

    return app
