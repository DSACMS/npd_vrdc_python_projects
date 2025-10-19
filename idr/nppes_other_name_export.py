
"""
==========
NPPES Other Name Full Export
==========

Extracts a list of the common domain names associated with provider email addresses, where that field is not null.


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


nppes_othername_export_filename = f"@~/nppes_othername_with_history.{ts}.csv"

nppes_IDR_main_export_sql = f"""
COPY INTO {nppes_othername_export_filename}""" + f"""
FROM (

    SELECT 
        PRVDR_NPI_NUM,
        NAME_ROLE_CD,
        NAME_ROLE_DESC,
        PRVDR_PREX_NAME,
        PRVDR_1ST_NAME,
        PRVDR_MDL_NAME,
        PRVDR_LAST_NAME,
        PRVDR_SFX_NAME,
        ORG_NAME,
        CRDNTL_TXT,
        TITLE_NAME,
        PHNE_NUM,
        PHNE_EXTNSN_NUM,
        EMAIL_ADR,
        SUBSTR(EMAIL_ADR, POSITION('@' IN EMAIL_ADR) + 1) AS domain_name,
        CASE WHEN YEAR(IDR_TRANS_OBSLT_TS) > 3000 THEN 1 ELSE 0 END AS is_current,
        YEAR(IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_YEAR,
        MONTH(IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_MONTH,
        YEAR(IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_YEAR,
        MONTH(IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_MONTH
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_OTHR_NAME_HSTRY

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

session.sql(nppes_IDR_main_export_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 