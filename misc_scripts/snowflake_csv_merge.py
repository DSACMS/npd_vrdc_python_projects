#!/usr/bin/env python3
"""
Written by ChatGPT in response to: 

I am going to have multiple different file patterns that I am downloading in this manner. 
Please write my a python script that will merge everything that has the same "root" 
from snowflakes download in the same directory. 

Cline used to add command line arguments afterwards. 
"""
import os
import glob
import re
import pandas as pd
import argparse
from collections import defaultdict
import sys

def print_red_warning(message):
    """
    Print a big red warning message to stderr
    """
    red_color = '\033[91m'  # ANSI red color code
    bold = '\033[1m'        # ANSI bold
    reset = '\033[0m'       # ANSI reset
    
    warning_box = f"""
{red_color}{bold}{'='*80}
🚨 WARNING: DUPLICATE COLUMN NAMES DETECTED! 🚨
{'='*80}{reset}

{red_color}{message}{reset}

{red_color}{bold}{'='*80}
This indicates your source CSV files have duplicate column headers.
Pandas automatically renamed them with .1, .2, etc. suffixes.
Please check your source data for duplicate column names!
{'='*80}{reset}
"""
    print(warning_box, file=sys.stderr, flush=True)

def detect_duplicate_columns(column_list):
    """
    Check if any columns have pandas auto-generated suffixes (.1, .2, etc.)
    Returns list of problematic columns
    """
    duplicate_columns = []
    for col in column_list:
        if re.search(r'\.\d+$', str(col)):
            duplicate_columns.append(col)
    return duplicate_columns

def group_files(files):
    """
    Group files by their root before the Snowflake part suffix (_0_0_0, etc.).
    E.g. 'foo_0_0_0.csv' and 'foo_0_0_1.csv' -> root 'foo'
    Also handles 'foo.csv_0_0_0.csv' -> root 'foo'
    """
    groups = defaultdict(list)
    for f in files:
        base = os.path.basename(f)
        # strip compression extension first
        if base.endswith(".gz"):
            base = base[:-3]
        if base.endswith(".csv"):
            base = base[:-4]

        # remove the trailing Snowflake suffix if present
        root = re.sub(r'(_\d+_\d+_\d+)$', '', base)
        
        # Handle case where root already ends with .csv (from files like filename.csv_0_0_0.csv)
        if root.endswith('.csv'):
            root = root[:-4]
        
        groups[root].append(f)
    return groups

def merge_group(root, files, outdir="."):
    # Sort files so order is consistent
    files = sorted(files)

    outname = os.path.join(outdir, f"{root}.csv")
    print(f"Merging {len(files)} files into {outname}")

    # Read first file with header to establish column structure
    df = pd.read_csv(files[0], compression="infer", dtype=str)
    expected_headers = list(df.columns)
    
    # Check for duplicate column indicators in the first file
    duplicate_cols = detect_duplicate_columns(expected_headers)
    if duplicate_cols:
        warning_msg = f"""
File: {files[0]}
Problematic columns detected: {duplicate_cols}

This suggests duplicate column names exist in your source CSV files.
Original column names were likely duplicated, causing pandas to auto-rename them.
"""
        print_red_warning(warning_msg)
    
    # Append others, validating headers match
    for f in files[1:]:
        df2 = pd.read_csv(f, compression="infer", dtype=str)
        
        # Check for duplicate column indicators in this file too
        file_duplicate_cols = detect_duplicate_columns(list(df2.columns))
        if file_duplicate_cols and file_duplicate_cols != duplicate_cols:
            warning_msg = f"""
File: {f}
Additional problematic columns detected: {file_duplicate_cols}

This file has different duplicate column patterns than the first file.
"""
            print_red_warning(warning_msg)
        
        # Validate headers match exactly
        if list(df2.columns) != expected_headers:
            raise ValueError(f"Header mismatch in file {f}. Expected: {expected_headers}, Got: {list(df2.columns)}")
        
        df = pd.concat([df, df2], ignore_index=True)

    # Final check on merged dataframe columns
    final_duplicate_cols = detect_duplicate_columns(list(df.columns))
    if final_duplicate_cols:
        warning_msg = f"""
Final merged file: {outname}
Final problematic columns: {final_duplicate_cols}

The merged output contains columns with .1, .2, etc. suffixes.
Review your source data to eliminate duplicate column names.
"""
        print_red_warning(warning_msg)

    df.to_csv(outname, index=False)
    print(f"Successfully merged {len(files)} files with {len(df)} total rows (excluding header)")

def main():
    parser = argparse.ArgumentParser(
        description="Merge CSV files with the same root name from Snowflake downloads"
    )
    parser.add_argument(
        "directory", 
        help="Directory location containing CSV files to merge"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for merged files (defaults to input directory)",
        default=None
    )
    
    args = parser.parse_args()
    
    # Validate directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        return 1
    
    # Set output directory
    output_dir = args.output_dir if args.output_dir else args.directory
    
    # Look for all csv/csv.gz files in specified directory
    csv_pattern = os.path.join(args.directory, "*.csv")
    gz_pattern = os.path.join(args.directory, "*.csv.gz")
    files = glob.glob(csv_pattern) + glob.glob(gz_pattern)
    
    if not files:
        print(f"No CSV files found in directory: {args.directory}")
        return 1
    
    print(f"Found {len(files)} CSV files in {args.directory}")
    groups = group_files(files)
    
    print(f"Identified {len(groups)} file groups to merge")
    for root, flist in groups.items():
        merge_group(root, flist, outdir=output_dir)
    
    print("Merge process completed")
    return 0

if __name__ == "__main__":
    main()
