#--Step20_CreateVTIN_PUF.py----------------------------
# presume that the VEIN class from VEIN.py  in scope and assume that MAIN_KEY and MAIN_MODULUS are also defined and in the scope of this databricks notebook
# Loop over every row of data in the results from Step10 in PUF_TIN_LIST
# replace the vtin column in each row of data with a proper value from VEIN.VTIN_identifier
# perform the UPDATE SQL operations on 100 rows at a time for efficiency
# spark.sql is already in scope, no need to import it.

rif_catalog = 'extracts'
rif_database = 'rif2025'

output_catalog = 'analytics'
output_database = 'dua_000000_ftr460'

BATCH_SIZE = 100


class VTINProcessor:
    """
    This class processes the PUF_TIN_LIST table to populate VTIN identifiers.
    All methods are static as this is a utility class for organizing sequential operations.
    """
    
    @staticmethod
    def _get_tin_records(*, output_catalog, output_database):
        """
        Retrieve all TIN records from the PUF_TIN_LIST table.
        Returns a list of Row objects with tin, vtin, cnt_bene_id, cnt_clm_id columns.
        """
        select_sql = f"""
        SELECT tin, vtin, cnt_bene_id, cnt_clm_id
        FROM {output_catalog}.{output_database}.PUF_TIN_LIST
        ORDER BY tin
        """
        
        print("Retrieving TIN records from PUF_TIN_LIST...")
        print(select_sql)
        
        result = spark.sql(select_sql)  # type: ignore
        return result.collect()
    
    @staticmethod
    def _generate_vtin_for_tin(*, tin_value, main_key, main_modulus):
        """
        Generate a VTIN identifier for a given TIN using the VEIN class.
        Returns the generated VTIN string.
        """
        try:
            vtin_identifier = VEIN.VTIN_identifier(  # type: ignore
                ein=tin_value,
                main_key=main_key,
                modulus=main_modulus
            )
            return vtin_identifier
        except Exception as e:
            print(f"Error generating VTIN for TIN {tin_value}: {str(e)}")
            return None
    
    @staticmethod
    def _create_batch_update_sql(*, tin_vtin_pairs, output_catalog, output_database):
        """
        Create a batch UPDATE SQL statement for updating multiple TIN records.
        Uses a CASE statement to update multiple records in a single operation.
        """
        if not tin_vtin_pairs:
            return None
        
        # Build the CASE statement for the SET clause
        case_statements = []
        tin_list = []
        
        for tin_value, vtin_value in tin_vtin_pairs:
            if vtin_value is not None:
                case_statements.append(f"WHEN tin = '{tin_value}' THEN '{vtin_value}'")
                tin_list.append(f"'{tin_value}'")
        
        if not case_statements:
            return None
        
        case_clause = "CASE " + " ".join(case_statements) + " END"
        where_clause = "tin IN (" + ", ".join(tin_list) + ")"
        
        update_sql = f"""
        UPDATE {output_catalog}.{output_database}.PUF_TIN_LIST
        SET vtin = {case_clause}
        WHERE {where_clause}
        """
        
        return update_sql
    
    @staticmethod
    def _process_tin_batch(*, tin_records_batch, main_key, main_modulus, output_catalog, output_database):
        """
        Process a batch of TIN records to generate VTINs and update the database.
        """
        tin_vtin_pairs = []
        
        # Generate VTINs for each TIN in the batch
        for record in tin_records_batch:
            tin_value = record['tin']
            vtin_identifier = VTINProcessor._generate_vtin_for_tin(
                tin_value=tin_value,
                main_key=main_key,
                main_modulus=main_modulus
            )
            
            if vtin_identifier is not None:
                tin_vtin_pairs.append((tin_value, vtin_identifier))
            else:
                print(f"Skipping TIN {tin_value} due to VTIN generation error")
        
        # Create and execute batch update SQL
        if tin_vtin_pairs:
            batch_update_sql = VTINProcessor._create_batch_update_sql(
                tin_vtin_pairs=tin_vtin_pairs,
                output_catalog=output_catalog,
                output_database=output_database
            )
            
            if batch_update_sql:
                return batch_update_sql
        
        return None
    
    @staticmethod
    def _create_batches(*, tin_records, batch_size):
        """
        Split the TIN records into batches of specified size.
        Returns a list of batches, where each batch is a list of records.
        """
        batches = []
        for i in range(0, len(tin_records), batch_size):
            batch = tin_records[i:i + batch_size]
            batches.append(batch)
        return batches
    
    @staticmethod
    def _display_sample_results(*, output_catalog, output_database, sample_size=10):
        """
        Display a sample of the updated PUF_TIN_LIST records to verify the updates.
        """
        sample_sql = f"""
        SELECT tin, vtin, cnt_bene_id, cnt_clm_id
        FROM {output_catalog}.{output_database}.PUF_TIN_LIST
        WHERE vtin != '                    '
        ORDER BY cnt_bene_id DESC
        LIMIT {sample_size}
        """
        
        print(f"\nDisplaying sample of updated records (limit {sample_size}):")
        print(sample_sql)
        
        result_df = spark.sql(sample_sql)  # type: ignore
        display(result_df)  # type: ignore
    
    @staticmethod
    def _get_update_statistics(*, output_catalog, output_database):
        """
        Get statistics on the VTIN update process.
        """
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN vtin != '                    ' THEN 1 END) as updated_records,
            COUNT(CASE WHEN vtin = '                    ' THEN 1 END) as blank_records
        FROM {output_catalog}.{output_database}.PUF_TIN_LIST
        """
        
        print("\nGetting update statistics...")
        print(stats_sql)
        
        result = spark.sql(stats_sql)  # type: ignore
        stats = result.collect()[0]
        
        total_records = stats['total_records']
        updated_records = stats['updated_records']
        blank_records = stats['blank_records']
        
        print(f"\nVTIN Update Statistics:")
        print(f"Total records: {total_records}")
        print(f"Updated records: {updated_records}")
        print(f"Blank records remaining: {blank_records}")
        print(f"Update success rate: {(updated_records/total_records)*100:.1f}%")
    
    @staticmethod
    def execute_vtin_processing(*, is_just_print, main_key, main_modulus):
        """
        Main execution method that orchestrates the entire VTIN processing workflow.
        Processes TIN records in batches to generate and update VTIN identifiers.
        """
        print("Starting VTIN processing for PUF_TIN_LIST...")
        
        # Step 1: Get all TIN records from the PUF_TIN_LIST table
        tin_records = VTINProcessor._get_tin_records(
            output_catalog=output_catalog,
            output_database=output_database
        )
        
        if not tin_records:
            print("No TIN records found in PUF_TIN_LIST. Exiting.")
            return
        
        print(f"Found {len(tin_records)} TIN records to process")
        
        # Step 2: Split records into batches
        batches = VTINProcessor._create_batches(
            tin_records=tin_records,
            batch_size=BATCH_SIZE
        )
        
        print(f"Processing {len(batches)} batches of up to {BATCH_SIZE} records each")
        
        # Step 3: Process each batch
        successful_batches = 0
        total_sql_statements = []
        
        for batch_index, batch in enumerate(batches, 1):
            print(f"\nProcessing batch {batch_index}/{len(batches)} ({len(batch)} records)")
            
            batch_update_sql = VTINProcessor._process_tin_batch(
                tin_records_batch=batch,
                main_key=main_key,
                main_modulus=main_modulus,
                output_catalog=output_catalog,
                output_database=output_database
            )
            
            if batch_update_sql:
                total_sql_statements.append(batch_update_sql)
                
                if is_just_print:
                    print(f"Batch {batch_index} SQL (printing only):")
                    print(batch_update_sql)
                    print("---")
                else:
                    print(f"Executing batch {batch_index} update...")
                    try:
                        spark.sql(batch_update_sql)  # type: ignore
                        successful_batches += 1
                        print(f"Batch {batch_index} completed successfully")
                    except Exception as e:
                        print(f"Error executing batch {batch_index}: {str(e)}")
            else:
                print(f"Batch {batch_index} skipped - no valid updates")
        
        # Step 4: Display results and statistics
        if not is_just_print:
            print(f"\nProcessed {successful_batches}/{len(batches)} batches successfully")
            
            VTINProcessor._get_update_statistics(
                output_catalog=output_catalog,
                output_database=output_database
            )
            
            VTINProcessor._display_sample_results(
                output_catalog=output_catalog,
                output_database=output_database
            )
        else:
            print(f"\nGenerated {len(total_sql_statements)} SQL statements for {len(batches)} batches")
        
        print("\nVTIN processing completed!")


# Execute the VTIN processing workflow
if __name__ == "__main__":
    # These variables should be defined in the Databricks notebook scope
    # MAIN_KEY and MAIN_MODULUS are presumed to be available
    is_just_print = True
    
    # Note: In actual Databricks execution, MAIN_KEY and MAIN_MODULUS should be defined
    # For this example, we're showing how they would be used
    try:
        VTINProcessor.execute_vtin_processing(
            is_just_print=is_just_print,
            main_key=MAIN_KEY,  # type: ignore
            main_modulus=MAIN_MODULUS  # type: ignore
        )
    except NameError as e:
        print(f"Missing required variables: {e}")
        print("MAIN_KEY and MAIN_MODULUS must be defined in the notebook scope")
