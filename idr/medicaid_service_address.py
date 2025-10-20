"""
==========
Medicaid Provider Service Address Export
==========

Simple export of TMSIS address data, and associated NPIs when they occur for more than a given threshold
of patients over a time period. Because address locations are particularly sensitive when they are NOT
actual hospital/clinic/snf/etc locations, we are requiring that this threshold be pretty high: 50 patients at least 
in the initial implementation. This might lower all the way to CMS standard 11 but for now we will keep it high.
Also we are looking for the most common service locations in any case.. it is not obvious that 
service locations which low patient volume have the same stability or usefulness. 

The following NPI columns are found on the claim line table: 

* CLM_LINE_OPRTG_PRVDR_NPI_NUM
* CLM_LINE_ORDRG_PRVDR_NPI_NUM


This code needs to have two npi lists.. one for the claim and one for the claim line.
The loop over the claim should not join to the claim line.. and should simply harvest the claim service 
address directly. 

The code for the claim line should join back to the main claim table in order to get the CLM_MBI_NUM in order to patient counts.
We would like to use COALESE to determine which address to take.. but sometimes the line2 of the claims line is null
which would mean we would take everything from the claim line.. except take the line2 from the claim.. which is wrong. 

So we will use a case statement that says: 

if the line1, state and zip from the claim line are not null.. then take all of the columns from the claim line.
Otherwise.. trust the claim level address. This case statement will need to be repeated for each address column. 

Lets also do a count distinct on CLM_UNIQ_ID on both the claim line and the claim to get a claim count alongside the patient count. 
use the same approach to get the "minimum" unique claim count.. despite our phased aggregation approach. 


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

medicaid_service_locations_file_name = f"@~/medicaid_provider_service_locations.{ts}.csv"

patient_aggregation_threshold = 20

# Dictionary of claim-level NPI fields to process separately
claim_npi_fields = {
    'CLM_RX_DSPNSNG_PRVDR_NPI_NUM': 'rx_dispensing_provider',
    'CLM_SPRVSNG_PRVDR_NPI_NUM': 'supervising_provider', 
    'CLM_SRVC_LCTN_ORG_NPI_NUM': 'service_location_org'
}

# Dictionary of claim-line level NPI fields to process separately  
claim_line_npi_fields = {
    'CLM_LINE_SRVCNG_PRVDR_NPI_NUM': 'line_servicing_provider',
    'CLM_LINE_SRVC_LCTN_ORG_NPI_NUM': 'line_service_location_org',
    'CLM_LINE_OPRTG_PRVDR_NPI_NUM': 'line_operating_provider',
    'CLM_LINE_ORDRG_PRVDR_NPI_NUM': 'line_ordering_provider'
}

# Step 1: Create temporary table to store combined NPI-address data
temp_table_name = f"TEMP_MEDICAID_NPI_ADDRESSES_{ts}"

# Step 2: Build individual queries for each NPI type and UNION them
union_queries = []

# Process claim-level NPIs (no join to claim line, use claim service address directly)
for npi_field, npi_type in claim_npi_fields.items():
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
        YEAR(CLM.CLM_FROM_DT) = 2025
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
for npi_field, npi_type in claim_line_npi_fields.items():
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
        YEAR(CLM.CLM_FROM_DT) = 2025
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
        CASE 
            WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
            THEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR 
            ELSE CLM.CLM_SRVC_LCTN_LINE_1_ADR 
        END,
        CASE 
            WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
            THEN CLINE.CLM_LINE_SRVC_LCTN_LINE_2_ADR 
            ELSE CLM.CLM_SRVC_LCTN_LINE_2_ADR 
        END,
        CASE 
            WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
            THEN CLINE.CLM_LINE_SRVC_LCTN_CITY_NAME 
            ELSE CLM.CLM_SRVC_LCTN_CITY_NAME 
        END,
        CASE 
            WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
            THEN CLINE.CLM_LINE_SRVC_LCTN_STATE_CD 
            ELSE CLM.CLM_SRVC_LCTN_STATE_CD 
        END,
        CASE 
            WHEN CLINE.CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_STATE_CD IS NOT NULL 
                AND CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD IS NOT NULL 
            THEN CLINE.CLM_LINE_SRVC_LCTN_ZIP_CD 
            ELSE CLM.CLM_SRVC_LCTN_ZIP_CD 
        END
    """
    union_queries.append(individual_query)

# Step 3: Create the temporary table with all NPI types combined
create_temp_table_sql = f"""
CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS (
    {' UNION ALL '.join(union_queries)}
)
"""

print("Creating temporary NPI-address table...")
session.sql(create_temp_table_sql).collect()

# Step 4: Final aggregation query with MAX logic and export
medicaid_service_locations_sql = f"""
COPY INTO {medicaid_service_locations_file_name}
FROM (
    SELECT 
        service_address_line_1,
        service_address_line_2,
        service_address_city,
        service_address_state,
        service_address_zip,
        MAX(patient_count) AS minimum_distinct_patient_count,
        MIN(claim_count) AS minimum_distinct_claim_count,
        COUNT(*) AS npi_address_combinations,
        LISTAGG(DISTINCT npi_column_type, '; ') AS npi_types_present
    FROM {temp_table_name}
    GROUP BY    
        service_address_line_1,
        service_address_line_2,
        service_address_city,
        service_address_state,
        service_address_zip
    HAVING MAX(patient_count) >= {patient_aggregation_threshold}
    ORDER BY minimum_distinct_patient_count DESC, minimum_distinct_claim_count DESC
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

print("Exporting final aggregated results...")
session.sql(medicaid_service_locations_sql).collect()

# Clean up temporary table
session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()
print(f"Export completed to: {medicaid_service_locations_file_name}")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
