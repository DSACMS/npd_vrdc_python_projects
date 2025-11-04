"""
==========
PECOS Current Provider Practice Addresses Per NPI 
==========

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

pecos_recent_address_file_name = f"@~/pecos_current_practice_address_w_date.{ts}.csv"

pecos_recent_address_sql = f"""
COPY INTO {pecos_recent_address_file_name}
FROM (
    SELECT 
        PRVDR_NPI_NUM,
        ADR_TYPE_DESC,
        LINE_1_ADR,
        LINE_2_ADR,
        GEO_USPS_STATE_CD,
        ADR_CITY_NAME,
        ZIP5_CD,
        ZIP4_CD,
        CNTRY_CD,
        PHNE_NUM,
        FAX_NUM,
        TO_DATE(IDR_UPDT_TS) AS IDR_UPDT_TS_DATE,
        TO_DATE(IDR_TRANS_OBSLT_TS) AS IDR_TRANS_OBSLT_TS_DATE,
        TO_DATE(IDR_TRANS_EFCTV_TS) AS IDR_TRANS_EFCTV_TS_DATE,
        CASE WHEN 
          YEAR(IDR_TRANS_OBSLT_TS) = 9999 THEN 1 
          ELSE 0 
          END AS is_current
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
    WHERE  ADR_TYPE_DESC = 'PRACTICE'
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

session.sql(pecos_recent_address_sql).collect()



# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 

