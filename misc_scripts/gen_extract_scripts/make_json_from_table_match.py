#!/usr/bin/env python3
"""
Snowflake Metadata Discovery Script
===================================

This script queries Snowflake's INFORMATION_SCHEMA to discover tables and views
matching a LIKE pattern, then generates a JSON file containing database, schema,
table/view names, and all column metadata.

Usage:
    Run this in a Snowflake notebook environment where you have an active session.
    
    python make_json_from_table_match.py --pattern '%PROVIDER%' --output metadata.json
    
Requirements:
    - Active Snowflake session
    - Access to INFORMATION_SCHEMA
    
Output JSON Structure:
    {
        "metadata": {
            "generated_timestamp": "2024-01-01T12:00:00",
            "search_pattern": "%PROVIDER%",
            "total_tables_found": 5
        },
        "tables": [
            {
                "database": "IDRC_PRD", 
                "schema": "CMS_VDM_VIEW_MDCR_PRD",
                "table_name": "V2_PROVIDER_DEMOGRAPHICS",
                "table_type": "VIEW",
                "columns": [
                    {
                        "column_name": "PRVDR_NPI_NUM",
                        "data_type": "VARCHAR",
                        "ordinal_position": 1,
                        "is_nullable": "YES"
                    }
                ]
            }
        ]
    }
"""

import json
import argparse
from datetime import datetime
from typing import Dict, List, Any

def discover_tables_and_columns(*, search_pattern: str) -> Dict[str, Any]:
    """
    Discovers tables/views matching the search pattern and their column metadata.
    
    Args:
        search_pattern: LIKE pattern for table matching (e.g., '%PROVIDER%')
        
    Returns:
        Dictionary containing metadata and table information
    """
    try:
        from snowflake.snowpark.context import get_active_session  # type: ignore
        session = get_active_session()
        
        print(f"Searching for tables matching pattern: {search_pattern}")
        
        # Query to find matching tables/views with case-insensitive LIKE
        tables_query = f"""
            SELECT DISTINCT
                table_catalog AS database_name,
                table_schema AS schema_name, 
                table_name,
                table_type
            FROM information_schema.tables
            WHERE UPPER(table_name) LIKE UPPER('{search_pattern}')
            ORDER BY database_name, schema_name, table_name
        """
        
        tables_result = session.sql(tables_query).collect()
        
        if not tables_result:
            print(f"No tables found matching pattern: {search_pattern}")
            return {
                "metadata": {
                    "generated_timestamp": datetime.now().isoformat(),
                    "search_pattern": search_pattern,
                    "total_tables_found": 0
                },
                "tables": []
            }
        
        print(f"Found {len(tables_result)} tables/views matching the pattern")
        
        tables_data = []
        
        for table_row in tables_result:
            database_name = table_row[0]
            schema_name = table_row[1]
            table_name = table_row[2]
            table_type = table_row[3]
            
            print(f"Processing: {database_name}.{schema_name}.{table_name}")
            
            # Query columns for this specific table, ordered by position in Snowflake
            columns_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    ordinal_position,
                    is_nullable
                FROM information_schema.columns
                WHERE table_catalog = '{database_name}'
                    AND table_schema = '{schema_name}'
                    AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            
            columns_result = session.sql(columns_query).collect()
            
            columns_data = []
            for col_row in columns_result:
                columns_data.append({
                    "column_name": col_row[0],
                    "data_type": col_row[1], 
                    "ordinal_position": col_row[2],
                    "is_nullable": col_row[3]
                })
            
            table_info = {
                "database": database_name,
                "schema": schema_name,
                "table_name": table_name,
                "table_type": table_type,
                "columns": columns_data
            }
            
            tables_data.append(table_info)
            print(f"  - Found {len(columns_data)} columns")
        
        result = {
            "metadata": {
                "generated_timestamp": datetime.now().isoformat(),
                "search_pattern": search_pattern,
                "total_tables_found": len(tables_data)
            },
            "tables": tables_data
        }
        
        print(f"Discovery complete. Found {len(tables_data)} tables/views total.")
        return result
        
    except Exception as e:
        print(f"make_json_from_table_match Error: Failed to discover table metadata")
        print(f"Error details: {str(e)}")
        print(f"Make sure you have an active Snowflake session and access to INFORMATION_SCHEMA")
        raise


def save_metadata_json(*, metadata: Dict[str, Any], output_file: str) -> None:
    """
    Saves metadata dictionary to JSON file with pretty formatting.
    
    Args:
        metadata: The metadata dictionary to save
        output_file: Path to output JSON file
    """
    try:
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=2, sort_keys=False)
        print(f"Metadata saved to: {output_file}")
        
        # Print summary
        table_count = metadata["metadata"]["total_tables_found"]
        total_columns = sum(len(table["columns"]) for table in metadata["tables"])
        
        print(f"\nSUMMARY:")
        print(f"========")
        print(f"Tables/Views Found: {table_count}")
        print(f"Total Columns: {total_columns}")
        print(f"Search Pattern: {metadata['metadata']['search_pattern']}")
        print(f"Generated: {metadata['metadata']['generated_timestamp']}")
        
        if table_count > 0:
            print(f"\nTables Found:")
            for table in metadata["tables"]:
                print(f"  - {table['database']}.{table['schema']}.{table['table_name']} ({table['table_type']}) - {len(table['columns'])} columns")
        
    except Exception as e:
        print(f"make_json_from_table_match Error: Failed to save JSON file")
        print(f"Error details: {str(e)}")
        raise


def run_metadata_discovery(*, search_pattern: str, output_file: str) -> Dict[str, Any]:
    """
    Main function for notebook usage.
    
    Args:
        search_pattern: LIKE pattern for table matching (e.g., '%PROVIDER%')
        output_file: Path to output JSON file
        
    Returns:
        Dictionary containing metadata and table information
    """
    print("Snowflake Metadata Discovery")
    print("===========================")
    print(f"Search Pattern: {search_pattern}")
    print(f"Output File: {output_file}")
    print("")
    
    try:
        # Discover tables and columns
        metadata = discover_tables_and_columns(search_pattern=search_pattern)
        
        # Save to JSON file
        save_metadata_json(metadata=metadata, output_file=output_file)
        
        print(f"\nSuccess! Metadata discovery completed.")
        return metadata
        
    except Exception as e:
        print(f"\nFailed to complete metadata discovery: {str(e)}")
        raise


# Example usage in notebook:
# 
# Set your parameters here:
# search_pattern = '%PROVIDER%'
# output_file = 'provider_metadata.json'
#
# Run the discovery:
# metadata = run_metadata_discovery(search_pattern=search_pattern, output_file=output_file)

if __name__ == '__main__':
    # Example parameters - modify these in your notebook
    search_pattern = '%PROVIDER%'  # Change this to your desired pattern
    output_file = 'metadata_output.json'  # Change this to your desired output file
    
    print("Running with example parameters...")
    print("Modify search_pattern and output_file variables for your use case.")
    
    run_metadata_discovery(search_pattern=search_pattern, output_file=output_file)
