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
session = get_active_session()

ts = datetime.now().strftime("%Y_%m_%d_%H%M")

medicaid_service_location_analysis_file_name = f"@~/medicaid_service_location_analysis.{ts}.csv"

print("Starting Medicaid Service Location Analysis...")

# Build the analysis query for service location combinations
medicaid_service_location_analysis_sql = f"""
COPY INTO {medicaid_service_location_analysis_file_name}
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
)""" + """
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = NONE
)
HEADER = TRUE
OVERWRITE = TRUE;
"""

print("Executing service location analysis query...")
session.sql(medicaid_service_location_analysis_sql).collect()

print(f"Analysis completed successfully!")
print(f"Export file: {medicaid_service_location_analysis_file_name}")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
