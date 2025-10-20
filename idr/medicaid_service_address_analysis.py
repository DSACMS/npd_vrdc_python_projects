"""
==========
Medicaid Provider Service Address Analysis
==========

Using the basic program flow found in idr/medicaid_service_address.py

Lets have a program which provids patient counts and distinct claim counts for the combinations of: 

        CLM_LINE_SRVC_LCTN_CITY_NAME,
        CLM_LINE_SRVC_LCTN_STATE_CD,
        CLM_LINE_SRVC_LCTN_ZIP_CD,

without consideration for any NPI at all. 
Then export this as a csv file.         

"""

# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import streamlit as st  # type: ignore
import pandas as pd
from datetime import datetime

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session # type: ignore

class MedicaidServiceAddressAnalysis:
    """Analysis of Medicaid service location combinations without NPI consideration."""
    
    def __init__(self):
        """Initialize the analysis with session and timestamp."""
        self.session = get_active_session()
        self.ts = datetime.now().strftime("%Y_%m_%d_%H%M")
        self.output_file_name = f"@~/medicaid_service_location_analysis.{self.ts}.csv"
        
    @staticmethod
    def _generate_analysis_query(*, file_name: str) -> str:
        """
        Generate SQL query for service location analysis.
        
        Args:
            file_name: Output file name for the COPY INTO statement
            
        Returns:
            Complete SQL query string
        """
        return f"""
COPY INTO {file_name}
FROM (
    SELECT 
        CLM_LINE_SRVC_LCTN_CITY_NAME,
        CLM_LINE_SRVC_LCTN_STATE_CD,
        CLM_LINE_SRVC_LCTN_ZIP_CD,
        COUNT(DISTINCT CLM_MBI_NUM) AS distinct_patient_count,
        COUNT(DISTINCT CLM.CLM_UNIQ_ID) AS distinct_claim_count
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM AS CLM 
    JOIN IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM_LINE AS CLINE ON 
        CLINE.CLM_UNIQ_ID = CLM.CLM_UNIQ_ID  
    WHERE 
        YEAR(CLM.CLM_FROM_DT) = 2025
        AND CLM_LINE_SRVC_LCTN_CITY_NAME IS NOT NULL
        AND CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL
        AND CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL
    GROUP BY    
        CLM_LINE_SRVC_LCTN_CITY_NAME,
        CLM_LINE_SRVC_LCTN_STATE_CD,
        CLM_LINE_SRVC_LCTN_ZIP_CD
    ORDER BY 
        distinct_patient_count DESC,
        distinct_claim_count DESC
)
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = NONE
)
HEADER = TRUE
OVERWRITE = TRUE;
"""

    @staticmethod
    def run_analysis():
        """Execute the complete service location analysis."""
        print("Starting Medicaid Service Location Analysis...")
        
        # Create analysis instance
        analysis = MedicaidServiceAddressAnalysis()
        
        # Generate and execute the analysis query
        print("Executing service location analysis query...")
        analysis_sql = analysis._generate_analysis_query(
            file_name=analysis.output_file_name
        )
        
        # Execute the query
        analysis.session.sql(analysis_sql).collect()
        
        print(f"Analysis completed successfully!")
        print(f"Export file: {analysis.output_file_name}")
        print("To download use:")
        print("snowsql -c cms_idr -q \"GET @~/ file://. PATTERN='.*.csv';\"")
        print("Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh")

# Execute the analysis when run directly
if __name__ == "__main__":
    MedicaidServiceAddressAnalysis.run_analysis()
