
from pathlib import Path
import sys

import pandas as pd

if len(sys.argv) != 2:
    print("Usage: python producer/inspect_dataset.py <path-to-parquet>")
    raise SystemExit(1)

file_path = Path(sys.argv[1])

if not file_path.is_file():
    raise FileNotFoundError(f"Dataset not found: {file_path}")

df = pd.read_parquet(file_path)

print("\n===== DATASET SUMMARY =====")
print("File:", file_path)
print("Rows:", len(df))
print("Columns:", len(df.columns))

print("\n===== COLUMN NAMES =====")
for column in df.columns:
    print(f"- {column}")

print("\n===== DATA TYPES =====")
print(df.dtypes.to_string())

print("\n===== FIRST 5 ROWS =====")
print(df.head().to_string())

print("\n===== MISSING VALUES =====")
print(df.isna().sum().to_string())

print("\n===== MEMORY USAGE =====")
print(f"{df.memory_usage(deep=True).sum() / (1024**2):.2f} MiB")
