# src/utils/get_emails.py

from src.db_utils import validate_query_file, query_to_df
from pathlib import Path
import pandas as pd
from dataclasses import dataclass


_QUERY = validate_query_file(Path('queries/DepartmentEmails.sql'))


class DepartmentNotFoundError(Exception):
    pass


class DuplicateDepartmentError(Exception):
    pass


@dataclass(frozen=True)
class DepartmentEmails:
    primary: str
    secondary: str | None = None


def get_emails(department_name: str) -> DepartmentEmails:
    """
    Extract a list of department emails from department name
    """
    df = query_to_df(_QUERY, params={'department_name': department_name})

    if df.empty:
        raise DepartmentNotFoundError(f"Department '{department_name}' not found")
    if len(df) > 1:
        raise DuplicateDepartmentError(f"Duplicate department '{department_name}'")

    required_cols = {'primary_email', 'secondary_email'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing columns: {required_cols - set(df.columns)}")

    row = df.iloc[0]

    if pd.isna(row['primary_email']):
        raise ValueError("Primary email is null")

    return DepartmentEmails(
        primary=str(row['primary_email']),
        secondary=str(row['secondary_email']) if pd.notna(row['secondary_email']) else None
    )


if __name__ == "__main__":

    # Example: python src/utils/get_department_emails.py marine
    import argparse
    parser = argparse.ArgumentParser(description="Fetch emails for a department")
    parser.add_argument('department', type=str, help="Department name")
    args = parser.parse_args()
    result = get_emails(args.department)

    print(f"Primary:    {result.primary}")
    print(f"Secondary:  {result.secondary}")

    # To use in other scripts:
    # from src.utils.get_emails import get_emails
    # result = get_emails('marine')
