"""
VRDC Entity Map Creation Runner

This notebook runner uses the VRDCEntityMapBuilder class to create entity maps
from VRDC claims data. Configure the variables below and run in Databricks.

Usage:
1. Import this file in a Databricks notebook
2. Modify configuration variables as needed  
3. Run the execution cells to create entity maps
"""

from CreateEntityMap import VRDCEntityMapBuilder
from entity_looper import MonthRange, VRDCEntityMapper

# Configuration Variables - Modify these for your environment
extracts_catalog = 'extracts'
output_catalog = 'analytics' 
output_database = 'dua_000000_ftr460'  # Replace with your database

# Entity Map Configuration
entity_map_view_name = 'vrdc_entity_map_view'
entity_map_table_name = 'vrdc_entity_map'
entity_log_table_name = 'vrdc_entity_map_log'

# Privacy threshold - minimum beneficiaries to include
cms_privacy_threshold = 10

# Time Range Configuration (modify these)
analysis_time_range = MonthRange(start_year=2023, start_month=1, end_year=2023, end_month=12)

# Settings to include (leave empty list for all settings)
include_settings = []  # ['bcarrier', 'dme', 'inpatient'] or [] for all

# Execution control
is_just_print = True  # Set to True to print SQL without executing


def main():
    """Main execution function for entity map creation."""
    
    print("VRDC Entity Map Builder")
    print("=" * 50)
    print(f"Time Range: {analysis_time_range}")
    
    # Handle settings
    settings_to_use = include_settings if include_settings else None
    if not include_settings:
        all_settings = VRDCEntityMapper.get_all_settings()
        print(f"Including all settings: {all_settings}")
    else:
        print(f"Including settings: {include_settings}")
    
    print(f"Privacy threshold: {cms_privacy_threshold} beneficiaries")
    print(f"Output: {output_catalog}.{output_database}")
    
    if is_just_print:
        print("\n⚠️  PRINT MODE - SQL will be displayed but not executed")
    
    # Build entity map
    sql_statements = VRDCEntityMapBuilder.build_entity_map(
        month_range=analysis_time_range,
        settings=settings_to_use,
        extracts_catalog=extracts_catalog,
        output_catalog=output_catalog,
        output_database=output_database,
        view_name=entity_map_view_name,
        table_name=entity_map_table_name,
        log_table_name=entity_log_table_name,
        privacy_threshold=cms_privacy_threshold,
        execute=not is_just_print
    )
    
    if not is_just_print:
        print("\n🎉 Entity map creation completed successfully!")
        print(f"\nView created: {output_catalog}.{output_database}.{entity_map_view_name}")
        print(f"Table created: {output_catalog}.{output_database}.{entity_map_table_name}")
        print(f"Log created: {output_catalog}.{output_database}.{entity_log_table_name}")
        
        # Display sample results
        display_sql = f"""
SELECT * FROM {output_catalog}.{output_database}.{entity_map_table_name} 
ORDER BY bene_count DESC 
LIMIT 10"""
        
        print(f"\nSample results:")
        print(display_sql)
        result_df = spark.sql(display_sql)  # type: ignore
        display(result_df)  # type: ignore
    
    return sql_statements


def print_configuration():
    """Display current configuration settings."""
    print("Current Configuration:")
    print("-" * 30)
    print(f"Time Range: {analysis_time_range}")
    print(f"Settings: {include_settings if include_settings else 'All 7 settings'}")
    print(f"Catalogs: {extracts_catalog} → {output_catalog}.{output_database}")
    print(f"View Name: {entity_map_view_name}")
    print(f"Table Name: {entity_map_table_name}")
    print(f"Log Table: {entity_log_table_name}")
    print(f"Privacy Threshold: {cms_privacy_threshold}")
    print(f"Print Mode: {is_just_print}")


def run_small_test():
    """Run a small test with limited time range and settings."""
    print("Running Small Test (January 2023, bcarrier + dme only)")
    print("=" * 60)
    
    test_range = MonthRange(start_year=2023, start_month=1, end_year=2023, end_month=1)
    
    test_sql = VRDCEntityMapBuilder.build_entity_map(
        month_range=test_range,
        settings=['bcarrier', 'dme'],
        extracts_catalog=extracts_catalog,
        output_catalog=output_catalog,
        output_database=output_database,
        view_name='test_entity_view',
        table_name='test_entity_table',
        log_table_name='test_entity_log',
        privacy_threshold=cms_privacy_threshold,
        execute=not is_just_print
    )
    
    print(f"Generated {len(test_sql)} SQL statements for test")
    return test_sql


# Notebook execution cells:

# Cell 1: Display Configuration
print_configuration()

# Cell 2: Optional - Run small test first  
# Uncomment the line below to run a small test
# run_small_test()

# Cell 3: Run full entity map creation
if __name__ == "__main__":
    main()
