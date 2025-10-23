"""
==========
Medicaid Provider Service Address Export (IDROutputter Implementation)
==========

Simple export of TMSIS address data, and associated NPIs when they occur for more than a given threshold
of patients over a time period. This is a complex export that creates temporary tables and performs
multi-stage data aggregation.

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""

# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session  # type: ignore

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class MedicaidServiceAddressExporter(IDROutputter):
    """
    Exports Medicaid provider service addresses using IDROutputter base class.
    
    This is a complex export that processes multiple NPI fields from both claim
    and claim-line tables, creating temporary aggregations before final export.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "medicaid_provider_service_locations"
    
    def __init__(self):
        # Configuration
        self.patient_aggregation_threshold = 20
        self.year = 2025
        
        # Temp table name will be generated when needed (not at import time)
        self.temp_table_name = None
        
        # Dictionary of claim-level NPI fields to process separately
        self.claim_npi_fields = {
            'CLM_RX_DSPNSNG_PRVDR_NPI_NUM': 'rx_dispensing_provider',
            'CLM_PRSCRBNG_PRVDR_NPI_NUM': 'rx_prescribing_provider',
            'CLM_SPRVSNG_PRVDR_NPI_NUM': 'supervising_provider', 
            'CLM_SRVC_LCTN_ORG_NPI_NUM': 'service_location_org',
            'CLM_ADMTG_PRVDR_NPI_NUM': 'admitting_provider',
            'CLM_BLG_PRVDR_NPI_NUM': 'billing_provider',
            'CLM_HLTH_HOME_PRVDR_NPI_NUM': 'home_health_provider',
            'CLM_ORDRG_PRVDR_NPI_NUM': 'ordering_provider'
        }

        # Dictionary of claim-line level NPI fields to process separately  
        self.claim_line_npi_fields = {
            'CLM_LINE_SRVCNG_PRVDR_NPI_NUM': 'line_servicing_provider',
            'CLM_LINE_SRVC_LCTN_ORG_NPI_NUM': 'line_service_location_org',
            'CLM_LINE_OPRTG_PRVDR_NPI_NUM': 'line_operating_provider',
            'CLM_LINE_ORDRG_PRVDR_NPI_NUM': 'line_ordering_provider'
        }
    
    def _generate_temp_table_name(self) -> str:
        """Generate unique temp table name when needed."""
        if self.temp_table_name is None:
            ts = datetime.now().strftime("%Y_%m_%d_%H%M")
            self.temp_table_name = f"TEMP_MEDICAID_NPI_ADDRESSES_{ts}"
        return self.temp_table_name
    
    def prepareData(self) -> None:
        """Create temporary table with all NPI-address combinations."""
        session = get_active_session()
        
        print("Creating temporary NPI-address table...")
        
        # Generate temp table name
        temp_table = self._generate_temp_table_name()
        
        union_queries = []
        
        # Process claim-level NPIs (no join to claim line, use claim service address directly)
        for npi_field, npi_type in self.claim_npi_fields.items():
            individual_query = f"""
            SELECT 
                {npi_field} AS npi,
                '{npi_type}' AS npi_column_type,
                CLM_SRVC_LCTN_LINE_1_ADR AS service_address_line_1,
                CLM_SRVC_LCTN_LINE_2_ADR AS service_address_line_2,
                CLM_SRVC_LCTN_CITY_NAME AS service_address_city,
                CLM_SRVC_LCTN_STATE_CD AS service_address_state,
                CLM_SRVC_LCTN_ZIP_CD AS service_address_zip,
                COUNT(DISTINCT CLM_MBI_NUM) AS patient_count,
                COUNT(DISTINCT CLM_UNIQ_ID) AS claim_count
            FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM AS CLM 
            WHERE 
                YEAR(CLM.CLM_FROM_DT) = {self.year}
                AND CLM_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLM_SRVC_LCTN_CITY_NAME IS NOT NULL
                AND CLM_SRVC_LCTN_STATE_CD IS NOT NULL
                AND CLM_SRVC_LCTN_ZIP_CD IS NOT NULL
                AND {npi_field} IS NOT NULL
            GROUP BY    
                {npi_field},
                CLM_SRVC_LCTN_LINE_1_ADR,
                CLM_SRVC_LCTN_LINE_2_ADR,
                CLM_SRVC_LCTN_CITY_NAME,
                CLM_SRVC_LCTN_STATE_CD,
                CLM_SRVC_LCTN_ZIP_CD
            """
            union_queries.append(individual_query)

        # Process claim-line NPIs (join back to main claim to get CLM_MBI_NUM, use CASE for address selection)
        for npi_field, npi_type in self.claim_line_npi_fields.items():
            individual_query = f"""
            SELECT 
                CLINE.{npi_field} AS npi,
                '{npi_type}' AS npi_column_type,
                CASE 
                    WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
                    THEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR 
                    ELSE CLM.CLM_SRVC_LCTN_LINE_1_ADR 
                END AS service_address_line_1,
                CASE 
                    WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
                    THEN CLINE.CLM_LINE_SRVC_LCTN_LINE_2_ADR 
                    ELSE CLM.CLM_SRVC_LCTN_LINE_2_ADR 
                END AS service_address_line_2,
                CASE 
                    WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
                    THEN CLINE.CLM_LINE_SRVC_LCTN_CITY_NAME 
                    ELSE CLM.CLM_SRVC_LCTN_CITY_NAME 
                END AS service_address_city,
                CASE 
                    WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
                    THEN CLINE.CLM_LINE_SRVC_LCTN_STATE_CD 
                    ELSE CLM.CLM_SRVC_LCTN_STATE_CD 
                END AS service_address_state,
                CASE 
                    WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
                    THEN CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD 
                    ELSE CLM.CLM_SRVC_LCTN_ZIP_CD 
                END AS service_address_zip,
                COUNT(DISTINCT CLM.CLM_MBI_NUM) AS patient_count,
                COUNT(DISTINCT CLINE.CLM_UNIQ_ID) AS claim_count
            FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM_LINE AS CLINE
            JOIN IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM AS CLM ON 
                CLINE.CLM_UNIQ_ID = CLM.CLM_UNIQ_ID  
            WHERE 
                YEAR(CLM.CLM_FROM_DT) = {self.year}
                AND CLINE.{npi_field} IS NOT NULL
                AND (
                    (CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_CITY_NAME IS NOT NULL
                        AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL)
                    OR 
                    (CLM.CLM_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                        AND CLM.CLM_SRVC_LCTN_CITY_NAME IS NOT NULL
                        AND CLM.CLM_SRVC_LCTN_STATE_CD IS NOT NULL 
                        AND CLM.CLM_SRVC_LCTN_ZIP_CD IS NOT NULL)
                )
            GROUP BY    
                CLINE.{npi_field},
                service_address_line_1,
                service_address_line_2,
                service_address_city,
                service_address_state,
                service_address_zip
            """
            union_queries.append(individual_query)

        # Create the temporary table with all NPI types combined
        create_temp_table_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS (
            {' UNION ALL '.join(union_queries)}
        )
        """
        
        session.sql(create_temp_table_sql).collect()
        
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for final aggregated Medicaid service address export.
        
        Aggregates the temporary table data with patient count thresholding.
        """
        temp_table = self._generate_temp_table_name()
        return f"""
        SELECT 
            npi,
            service_address_line_1,
            service_address_line_2,
            service_address_city,
            service_address_state,
            service_address_zip,
            MAX(patient_count) AS minimum_distinct_patient_count,
            MIN(claim_count) AS minimum_distinct_claim_count,
            COUNT(*) AS npi_address_combinations,
            LISTAGG(DISTINCT npi_column_type, '; ') AS npi_types_present
        FROM {temp_table}
        GROUP BY    
            npi,
            service_address_line_1,
            service_address_line_2,
            service_address_city,
            service_address_state,
            service_address_zip
        HAVING MAX(patient_count) >= {self.patient_aggregation_threshold}
        ORDER BY minimum_distinct_patient_count DESC, minimum_distinct_claim_count DESC
        """
    
    def do_idr_output(self) -> None:
        """Override to handle cleanup of temporary table."""
        try:
            # Call parent implementation
            super().do_idr_output()
        finally:
            # Clean up temporary table
            session = get_active_session()
            session.sql(f"DROP TABLE IF EXISTS {self.temp_table_name}").collect()


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    print("Creating temporary NPI-address table...")
    exporter = MedicaidServiceAddressExporter()
    # First run data preparation 
    exporter.prepareData()
    print("Exporting final aggregated results...")
    exporter.do_idr_output()
    print("Export completed successfully!")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
