from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DATE_COLUMN_CANDIDATES = ["DATE", "Date"]
MONTH_COLUMN_NAME = "Months"


def is_valid_date(value: object) -> bool:
    text = str(value).strip()
    if not text:
        return False

    # Accept both mm-dd-yy and mm/dd/yy formats.
    for fmt in ("%m-%d-%y", "%m/%d/%y"):
        try:
            pd.to_datetime(text, format=fmt)
            return True
        except (ValueError, TypeError):
            continue
    return False


def is_valid_month_year(value: object) -> bool:
    text = str(value).strip()
    if not text:
        return False
    try:
        pd.to_datetime(text, format="%b-%y")
        return True
    except (ValueError, TypeError):
        return False


def preprocess_csv(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv, low_memory=False)
    original_count = len(df)

    # For date-related columns:
    # 1) remove empty/null values
    # 2) remove rows not matching expected patterns
    date_cols_to_check = [col for col in DATE_COLUMN_CANDIDATES if col in df.columns]

    for col in date_cols_to_check:
        non_empty_mask = df[col].notna() & df[col].astype(str).str.strip().ne("")
        valid_mask = df[col].apply(is_valid_date)
        df = df[non_empty_mask & valid_mask]

    if MONTH_COLUMN_NAME in df.columns:
        non_empty_mask = df[MONTH_COLUMN_NAME].notna() & df[MONTH_COLUMN_NAME].astype(str).str.strip().ne("")
        valid_mask = df[MONTH_COLUMN_NAME].apply(is_valid_month_year)
        df = df[non_empty_mask & valid_mask]

    cleaned_count = len(df)
    removed_count = original_count - cleaned_count

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)

    print(f"Input file:  {input_csv}")
    print(f"Output file: {output_csv}")
    print(f"Original rows: {original_count}")
    print(f"Cleaned rows:  {cleaned_count}")
    print(f"Removed rows:  {removed_count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess sales CSV file")
    parser.add_argument(
        "--input-csv",
        default="/home/ntlpt19/personal_projects/sales_rag/Sales Dataset/Sales Dataset/Amazon Sale Report.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output-csv",
        default="/home/ntlpt19/personal_projects/sales_rag/Sales Dataset/Sales Dataset/Amazon Sale Report_2.csv",
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    preprocess_csv(Path(args.input_csv), Path(args.output_csv))


if __name__ == "__main__":
    main()
