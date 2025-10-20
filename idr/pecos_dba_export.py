"""
==========
PECOS DBA Name History
==========

Note that a LEFT JOIN here does return more results.. 
but I am not sure what that means, so I am keeping the cleaner INNER JOIN.
TODO: figure out why a LEFT JOIN has more results and figure out what that means. 

"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import streamlit as st # type: ignore
import pandas as pd
from datetime import datetime

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session # type: ignore
session = get_active_session()

ts = datetime.now().strftime("%Y_%m_%d_%H%M")

pecos_dba_history_file_name = f"@~/pecos_dba_history.{ts}.csv"


now = datetime.now()
last_year = now.year - 1 


pecos_dba_history_sql = f"""
COPY INTO {pecos_dba_history_file_name}
FROM (

  SELECT 
    PRVDR_NPI_NUM,
    dba_history.PRVDR_ORG_NAME,
    enrollment_to_npi.IDR_TRANS_EFCTV_TS AS enrollment_to_npi_IDR_TRANS_EFCTV_TS,
    enrollment_to_npi.IDR_TRANS_OBSLT_TS AS enrollment_to_npi_IDR_TRANS_OBSLT_TS,
    dba_history.PRVDR_ENRLMT_ID,
    dba_history.PRVDR_PAC_ID,
    dba_history.PRVDR_ENRLMT_ASCTN_ROLE_CD,
    dba_history.PRVDR_ENRLMT_ASCTN_ROLE_DESC,
    dba_history.IDR_TRANS_EFCTV_TS AS dba_history_IDR_TRANS_EFCTV_TS,
    dba_history.IDR_TRANS_OBSLT_TS AS dba_history_IDR_TRANS_OBSLT_TS


  FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_ASCTN_DBA_NAME_HSTRY  AS dba_history
      JOIN  IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_NPI_HSTRY AS enrollment_to_npi ON
        enrollment_to_npi.prvdr_enrlmt_id =
        dba_history.prvdr_enrlmt_id


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

session.sql(pecos_dba_history_sql).collect()


# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 