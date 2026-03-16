"""
main.py — Entry point for the Data Quality Improvement System.

Usage:
    python main.py <input_file> [--output-dir <dir>]

Examples:
    python main.py data/customers.csv
    python main.py data/orders.json --output-dir results
"""
import sys
import argparse

from reader   import read_file
from detector import detect_types, print_type_summary
from checker  import check
from cleaner  import clean
from reporter import build_report, report_to_text
from writer   import write_output


def main(input_path: str, output_dir: str = "output") -> None:
    print(f"\n[1/6] Reading input: {input_path}")
    df = read_file(input_path)
    total_rows, total_cols = df.shape
    print(f"      Loaded {total_rows} rows × {total_cols} columns.")

    print("\n[2/6] Detecting column types...")
    col_types = detect_types(df)
    print_type_summary(col_types)

    print("[3/6] Checking data quality...")
    issues = check(df, col_types)
    print(f"      Found {len(issues)} issue(s).")

    print("\n[4/6] Cleaning data...")
    cleaned_df, change_records = clean(df, issues, col_types)
    fixed   = sum(1 for r in change_records if r.fixed)
    flagged = sum(1 for r in change_records if not r.fixed)
    print(f"      Auto-fixed: {fixed}  |  Flagged for review: {flagged}")

    print("\n[5/6] Building report...")
    report = build_report(change_records, total_rows, total_cols)

    print("\n[6/6] Writing output...")
    cleaned_path, txt_path, json_path = write_output(
        cleaned_df, report, input_path, output_dir
    )
    print(f"      Cleaned data -> {cleaned_path}")
    print(f"      Report (txt) -> {txt_path}")
    print(f"      Report (json)-> {json_path}")

    print("\n" + report_to_text(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality Improvement System")
    parser.add_argument("input_file", help="Path to the input data file (csv/json/xml/yaml)")
    parser.add_argument(
        "--output-dir", default="output",
        help="Directory to write cleaned data and reports (default: output)"
    )
    args = parser.parse_args()
    main(args.input_file, args.output_dir)
