"""Data backup and safe write operations."""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataSafetyManager:
    """Manages data safety operations including backups, validation, and safe writes."""

    def __init__(self, data_dir: str = "data", backup_dir: str = "data/backups"):
        self.data_dir = Path(data_dir)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)

        # Critical files that need extra protection
        self.critical_files = {
            "fpl_data.db",  # Main database file
            "fpl_raw_bootstrap.json",  # Raw API data backup
            "fpl_raw_fixtures.json",  # Raw fixtures backup
        }

    def create_backup(self, filename: str, backup_suffix: str = None) -> Path:
        """Create a timestamped backup of a file."""
        source_path = self.data_dir / filename

        if not source_path.exists():
            logger.warning(f"File {filename} does not exist, skipping backup")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{backup_suffix}" if backup_suffix else ""
        backup_filename = f"{source_path.stem}{suffix}_{timestamp}{source_path.suffix}"
        backup_path = self.backup_dir / backup_filename

        shutil.copy2(source_path, backup_path)
        logger.info(f"Created backup: {backup_path}")
        return backup_path

    def create_full_backup(self, backup_suffix: str = "full_backup") -> dict[str, Path]:
        """Create backups of all critical files."""
        backups = {}
        for filename in self.critical_files:
            backup_path = self.create_backup(filename, backup_suffix)
            if backup_path:
                backups[filename] = backup_path

        logger.info(f"Created full backup of {len(backups)} files")
        return backups

    def get_file_hash(self, filepath: Path) -> str:
        """Calculate MD5 hash of a file for integrity checking."""
        if not filepath.exists():
            return None

        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def validate_file_integrity(self, filename: str, expected_hash: str = None) -> bool:
        """Validate file integrity using hash comparison."""
        filepath = self.data_dir / filename
        if not filepath.exists():
            logger.error(f"File {filename} does not exist")
            return False

        current_hash = self.get_file_hash(filepath)

        if expected_hash and current_hash != expected_hash:
            logger.error(f"File {filename} integrity check failed")
            return False

        return True

    def safe_backup_database(self, backup_suffix: str = None) -> bool:
        """Safely backup the database with validation."""
        db_filename = "fpl_data.db"
        db_path = self.data_dir / db_filename

        if not db_path.exists():
            logger.warning(f"Database file {db_filename} does not exist, skipping backup")
            return False

        try:
            # Create timestamped backup
            backup_path = self.create_backup(db_filename, backup_suffix or "db_backup")

            if backup_path:
                # Verify backup integrity
                original_hash = self.get_file_hash(db_path)
                backup_hash = self.get_file_hash(backup_path)

                if original_hash == backup_hash:
                    logger.info(f"Database backup verified: {backup_path}")
                    return True
                else:
                    logger.error("Database backup integrity check failed")
                    return False
            else:
                logger.error("Failed to create database backup")
                return False

        except Exception as e:
            logger.error(f"Error during database backup: {e}")
            return False

    def get_data_summary(self) -> dict[str, dict]:
        """Get summary statistics for database and critical files."""
        summary = {}

        for filename in self.critical_files:
            filepath = self.data_dir / filename
            if filepath.exists():
                try:
                    if filename.endswith(".db"):
                        # Database file summary
                        from sqlalchemy import text

                        from db.database import SessionLocal

                        try:
                            session = SessionLocal()
                            try:
                                # Get table count
                                table_result = session.execute(
                                    text(
                                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                                    )
                                )
                                table_count = table_result.scalar()

                                summary[filename] = {
                                    "type": "database",
                                    "tables": table_count,
                                    "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
                                    "last_modified": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                                    "hash": self.get_file_hash(filepath),
                                }
                            finally:
                                session.close()
                        except Exception as db_error:
                            summary[filename] = {
                                "type": "database",
                                "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
                                "last_modified": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                                "error": f"Database connection failed: {db_error}",
                            }
                    elif filename.endswith(".json"):
                        # JSON file summary
                        import json

                        with open(filepath) as f:
                            data = json.load(f)

                        summary[filename] = {
                            "type": "json",
                            "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
                            "last_modified": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                            "hash": self.get_file_hash(filepath),
                            "structure": list(data.keys()) if isinstance(data, dict) else "array",
                        }
                    else:
                        # Generic file summary
                        summary[filename] = {
                            "type": "file",
                            "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
                            "last_modified": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                            "hash": self.get_file_hash(filepath),
                        }

                except Exception as e:
                    summary[filename] = {"error": str(e)}
            else:
                summary[filename] = {"status": "missing"}

        return summary

    def cleanup_old_backups(self, keep_days: int = 7):
        """Remove backup files older than specified days."""
        cutoff_time = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)
        removed_count = 0

        for backup_file in self.backup_dir.glob("*"):
            if backup_file.stat().st_mtime < cutoff_time:
                backup_file.unlink()
                removed_count += 1

        logger.info(f"Cleaned up {removed_count} old backup files")

    def emergency_restore(self, filename: str, backup_timestamp: str = None) -> bool:
        """Restore a file from the most recent (or specified) backup."""
        if backup_timestamp:
            # Restore from specific backup
            backup_pattern = f"{Path(filename).stem}*{backup_timestamp}*"
        else:
            # Find most recent backup
            backup_pattern = f"{Path(filename).stem}*"

        backup_files = list(self.backup_dir.glob(backup_pattern))

        if not backup_files:
            logger.error(f"No backup found for {filename}")
            return False

        # Get most recent backup
        latest_backup = max(backup_files, key=lambda x: x.stat().st_mtime)

        try:
            target_path = self.data_dir / filename
            shutil.copy2(latest_backup, target_path)
            logger.info(f"Restored {filename} from {latest_backup}")
            return True
        except Exception as e:
            logger.error(f"Failed to restore {filename}: {e}")
            return False


# Global instance
data_safety = DataSafetyManager()


def safe_database_backup(backup_suffix: str = None) -> bool:
    """Convenience function for safe database backup."""
    return data_safety.safe_backup_database(backup_suffix)


def create_safety_backup(backup_suffix: str = "safety_backup") -> dict[str, Path]:
    """Convenience function for creating full backup."""
    return data_safety.create_full_backup(backup_suffix)
