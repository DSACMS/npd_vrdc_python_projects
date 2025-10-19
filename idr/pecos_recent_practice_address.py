"""
Current PECOS Practice Addresses Per NPI 

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

address_file_name = f"@~/pecos_recent_practice_address.{ts}.csv"

address_sql = f"""
COPY INTO {address_file_name}
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
    YEAR(IDR_UPDT_TS) AS IDR_UPDT_TS_YEAR,
    YEAR(IDR_TRANS_OBSLT_TS) AS IDR_TRANS_OBSLT_TS_YEAR,
    YEAR(IDR_TRANS_EFCTV_TS) AS IDR_TRANS_EFCTV_TS_YEAR
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
WHERE YEAR(IDR_TRANS_OBSLT_TS) = 9999 AND YEAR(IDR_TRANS_EFCTV_TS) > 2022 AND ADR_TYPE_DESC = 'PRACTICE'
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

session.sql(address_sql).collect()


summary_address_file_name = f"@~/pecos_address_summary.{ts}.csv"

summary_address_sql = f"""
COPY INTO {summary_address_file_name}
FROM (
SELECT 
    PRVDR_NPI_NUM,
    GEO_USPS_STATE_CD,
    ADR_CITY_NAME,
    ZIP5_CD,
    ZIP4_CD
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
WHERE YEAR(IDR_TRANS_OBSLT_TS) = 9999 AND YEAR(IDR_TRANS_EFCTV_TS) > 2022 AND ADR_TYPE_DESC = 'PRACTICE'
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

session.sql(summary_address_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 

