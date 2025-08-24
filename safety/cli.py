"""CLI interface for data safety operations."""

import typer

from .backup import create_safety_backup, data_safety, safe_database_backup
from .integrity import validate_data_integrity, validate_raw_data_completeness


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
        """Show summary of database and critical files."""
        summary = data_safety.get_data_summary()
        typer.echo("📊 Data Summary:")
        for filename, info in summary.items():
            if "error" in info:
                typer.echo(f"  ❌ {filename}: {info['error']}")
            elif "status" in info:
                typer.echo(f"  ⚠️  {filename}: {info['status']}")
            elif info.get("type") == "database":
                typer.echo(f"  🗄️  {filename}: {info.get('tables', '?')} tables, {info['size_mb']} MB")
            elif info.get("type") == "json":
                typer.echo(f"  📄 {filename}: JSON data, {info['size_mb']} MB")
            else:
                typer.echo(f"  📁 {filename}: {info['size_mb']} MB")

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

    @app.command()
    def completeness():
        """Show raw data capture completeness statistics."""
        results = validate_raw_data_completeness()
        typer.echo("📊 Raw Data Capture Completeness:")

        if "error" in results:
            typer.echo(f"  ❌ Error: {results['error']}")
            return

        for table, stats in results.items():
            if isinstance(stats, dict):
                completeness = stats.get("completeness_percent", 0)
                rows = stats.get("row_count", 0)
                captured = stats.get("columns_captured", 0)
                expected = stats.get("expected_columns", 0)

                status = "✅" if completeness >= 95 else "⚠️" if completeness >= 80 else "❌"
                typer.echo(
                    f"  {status} {table}: {completeness}% complete ({captured}/{expected} fields, {rows:,} rows)"
                )

    @app.command()
    def backup_db(suffix: str = typer.Option("manual_db_backup", help="Backup suffix")):
        """Create a backup of the database file."""
        success = safe_database_backup(suffix)
        if success:
            typer.echo("✅ Database backup created successfully")
        else:
            typer.echo("❌ Failed to create database backup")

    return app
