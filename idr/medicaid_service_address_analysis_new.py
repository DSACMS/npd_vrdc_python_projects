"""
==========
Medicaid Provider Service Address Analysis (IDROutputter Implementation)
==========

Using the basic program flow found in idr/medicaid_service_address.py

Lets have a program which provids patient counts and distinct claim counts for the combinations of: 

        CLM_LINE_SRVC_LCTN_CITY_NAME,
        CLM_LINE_SRVC_LCTN_STATE_CD,
        CLM_LINE_SRVC_LCTN_ZIP_CD,

without consideration for any NPI at all. 
Then export this as a csv file.         

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""

# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd
from datetime import datetime

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class MedicaidServiceAddressAnalysisExporter(IDROutputter):
    """
    Exports Medicaid service address analysis using IDROutputter base class.
    
    Analyzes patient counts and distinct claim counts by service location
    combinations without consideration for any NPI.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "medicaid_service_location_analysis"
    
    def __init__(self):
        # Set analysis year - can be overridden by subclasses
        self.year = 2025
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for Medicaid service address analysis.
        
        Combines ClaimLine and Claim tables to analyze service location patterns
        with patient counts and claim counts by city, state, and zip combinations.
        """
        return f"""
    -- Query ClaimLine table with original column names
    SELECT 
        CLM_LINE_SRVC_LCTN_CITY_NAME AS service_location_city_name,
        CLM_LINE_SRVC_LCTN_STATE_CD AS service_location_state_cd,
        CLM_LINE_SRVC_LCTN_ZIP_CD AS service_location_zip_cd,
        COUNT(DISTINCT CLM_UNIQ_ID) AS max_patient_count,
        COUNT(DISTINCT CLM_UNIQ_ID) AS distinct_claim_count,
        'ClaimLine' AS source_table
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM_LINE AS CLINE
    WHERE 
        YEAR(CLM_FROM_DT) = {self.year}
        AND CLM_LINE_SRVC_LCTN_CITY_NAME IS NOT NULL
        AND CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL
        AND CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL
    GROUP BY    
        CLM_LINE_SRVC_LCTN_CITY_NAME,
        CLM_LINE_SRVC_LCTN_STATE_CD,
        CLM_LINE_SRVC_LCTN_ZIP_CD
    
    UNION ALL
    
    -- Query Claim table with modified column names (remove "_LINE_")
    SELECT 
        CLM_SRVC_LCTN_CITY_NAME AS service_location_city_name,
        CLM_SRVC_LCTN_STATE_CD AS service_location_state_cd,
        CLM_SRVC_LCTN_ZIP_CD AS service_location_zip_cd,
        COUNT(DISTINCT CLM_MBI_NUM) AS max_patient_count,
        COUNT(DISTINCT CLM_UNIQ_ID) AS distinct_claim_count,
        'Claim' AS source_table
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM AS CLM
    WHERE 
        YEAR(CLM_FROM_DT) = {self.year}
        AND CLM_SRVC_LCTN_CITY_NAME IS NOT NULL
        AND CLM_SRVC_LCTN_STATE_CD IS NOT NULL
        AND CLM_SRVC_LCTN_ZIP_CD IS NOT NULL
    GROUP BY    
        CLM_SRVC_LCTN_CITY_NAME,
        CLM_SRVC_LCTN_STATE_CD,
        CLM_SRVC_LCTN_ZIP_CD
    
    ORDER BY 
        max_patient_count DESC,
        distinct_claim_count DESC
        """


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    print("Starting Medicaid Service Location Analysis...")
    exporter = MedicaidServiceAddressAnalysisExporter()
    exporter.do_idr_output()
    print("Analysis completed successfully!")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
