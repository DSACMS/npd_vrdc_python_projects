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

def group_files(files):
    """
    Group files by their root before the Snowflake part suffix (_0_0_0, etc.).
    E.g. 'foo_0_0_0.csv' and 'foo_0_0_1.csv' -> root 'foo'
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
        groups[root].append(f)
    return groups

def merge_group(root, files, outdir="."):
    # Sort files so order is consistent
    files = sorted(files)

    outname = os.path.join(outdir, f"{root}.merged.csv")
    print(f"Merging {len(files)} files into {outname}")

    # Read first file with header to establish column structure
    df = pd.read_csv(files[0], compression="infer", dtype=str)
    expected_headers = list(df.columns)
    
    # Append others, validating headers match
    for f in files[1:]:
        df2 = pd.read_csv(f, compression="infer", dtype=str)
        
        # Validate headers match exactly
        if list(df2.columns) != expected_headers:
            raise ValueError(f"Header mismatch in file {f}. Expected: {expected_headers}, Got: {list(df2.columns)}")
        
        df = pd.concat([df, df2], ignore_index=True)

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
