"""Validation utilities and functions."""

import pandas as pd
import pandera as pa


def validate_dataframe(df: pd.DataFrame, schema: pa.DataFrameModel, name: str) -> pd.DataFrame:
    """Validate a DataFrame against a Pandera schema with helpful error messages."""
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        print(f"❌ Validation failed for {name}:")
        for error in e.failure_cases.itertuples():
            print(f"  - {error.check} failed: {error.failure_case}")
        raise
    except Exception as e:
        print(f"❌ Validation error for {name}: {e}")
        raise
