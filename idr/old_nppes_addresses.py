# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import streamlit as st  # type: ignore
import pandas as pd
from datetime import datetime

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session # type: ignore
session = get_active_session()

ts = datetime.now().strftime("%Y_%m_%d_%H%M")

medicaid_credentials_file_name = f"@~/medicaid_credentials.{ts}.csv"

medicaid_credentials_sql = f"""
COPY INTO {medicaid_credentials_file_name}""" + """
FROM (
SELECT 
    address_history_outside.PRVDR_NPI_NUM,
    address_history_outside.LINE_1_ADR,
    address_history_outside.LINE_2_ADR,
    address_history_outside.ADR_CITY_NAME,
    address_history_outside.GEO_USPS_STATE_CD,
    address_history_outside.ZIP5_CD,
    address_history_outside.ZIP4_CD,
    address_history_outside.CNTRY_CD,
    address_history_outside.PHNE_NUM,
    address_history_outside.FAX_NUM,
    YEAR(address_history_outside.IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_YEAR,
    MONTH(address_history_outside.IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_MONTH,
    YEAR(address_history_outside.IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_YEAR,
    MONTH(address_history_outside.IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_MONTH
FROM (
    SELECT PRVDR_NPI_NUM 
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
    WHERE ADR_TYPE_CD = '03'
    GROUP BY PRVDR_NPI_NUM
    HAVING COUNT(*) > 1
) AS npis_with_at_least_two_practice_addresses
LEFT JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY AS address_history_outside ON (
    address_history_outside.PRVDR_NPI_NUM = 
    npis_with_at_least_two_practice_addresses.PRVDR_NPI_NUM
    AND 
    address_history_outside.ADR_TYPE_CD = '03'
    )
WHERE YEAR(address_history_outside.IDR_TRANS_OBSLT_TS) < 9000
ORDER BY address_history_outside.PRVDR_NPI_NUM DESC
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

session.sql(medicaid_credentials_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 