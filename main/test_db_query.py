from __future__ import annotations

import argparse
import sqlite3
from typing import Iterable


def print_rows(title: str, columns: list[str], rows: Iterable[sqlite3.Row], max_rows: int = 10) -> None:
    rows = list(rows)
    print(f"\n{title}")
    if not rows:
        print("  No rows returned.")
        return

    print("  Columns:", ", ".join(columns))
    for i, row in enumerate(rows[:max_rows], start=1):
        print(f"  {i}.", {col: row[col] for col in columns})


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def run_manual_queries(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA temp_store = MEMORY;")

    try:
        table_name = "amazon_sale_report"
        if not table_exists(conn, table_name):
            print(f"Table '{table_name}' not found in {db_path}")
            return

        print(f"Connected to DB: {db_path}")
        print(f"Using table: {table_name}")

        # 1) Count rows
        row = conn.execute("SELECT COUNT(*) AS total_rows FROM amazon_sale_report;").fetchone()
        print(f"\nTotal rows in amazon_sale_report: {row['total_rows']}")

        # 2) Sample records
        sample_rows = conn.execute(
            """
            SELECT order_id, date, status, category, qty, amount
            FROM amazon_sale_report
            LIMIT 5;
            """
        ).fetchall()
        print_rows("Sample rows", ["order_id", "date", "status", "category", "qty", "amount"], sample_rows)

        # 3) Manual query: top categories by total amount (overall)
        top_categories = conn.execute(
            """
            SELECT category, SUM(CAST(amount AS REAL)) AS total_amount
            FROM amazon_sale_report
            WHERE amount IS NOT NULL
            GROUP BY category
            ORDER BY total_amount DESC
            LIMIT 10;
            """
        ).fetchall()
        print_rows("Top categories by total_amount", ["category", "total_amount"], top_categories)

        # 4) Manual query: April underperformers in latest available year (MM-DD-YY date format)
        april_underperformers = conn.execute(
            """
            WITH latest_year AS (
              SELECT MAX(substr(date, 7, 2)) AS yy
              FROM amazon_sale_report
              WHERE date IS NOT NULL
            ),
            april_sales AS (
              SELECT
                category,
                SUM(CAST(amount AS REAL)) AS total_amount
              FROM amazon_sale_report
              WHERE date IS NOT NULL
                AND substr(date, 1, 2) = '04'
                AND substr(date, 7, 2) = (SELECT yy FROM latest_year)
                AND status NOT LIKE 'Cancelled%'
                AND COALESCE(qty, 0) > 0
              GROUP BY category
            ),
            avg_sales AS (
              SELECT AVG(total_amount) AS avg_amount FROM april_sales
            )
            SELECT category, total_amount
            FROM april_sales
            WHERE total_amount < (SELECT avg_amount FROM avg_sales)
            ORDER BY total_amount ASC;
            """
        ).fetchall()
        print(april_underperformers)
        print_rows(
            "April underperformers (below average total_amount in latest year)",
            ["category", "total_amount"],
            april_underperformers,
            max_rows=50,
        )

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual SQLite checks for amazon_sale_report")
    parser.add_argument("--db-path", default="./retail_insights.db", help="SQLite DB path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_manual_queries(args.db_path)


if __name__ == "__main__":
    main()
