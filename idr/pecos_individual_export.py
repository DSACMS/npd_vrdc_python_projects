
"""
==========
PECOS Individual Export
==========

Exports the PAC_IDs and the NPIs so that we can leverage the PAC_IDs as a mechanism to represent organizations that is already public.
See: 

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


nppes_IDR_main_export_filename = f"@~/pecos_individual_export.{ts}.csv"

nppes_IDR_main_export_sql = f"""
COPY INTO {nppes_IDR_main_export_filename}""" + f"""
FROM (


SELECT 
    PRVDR_PAC_ID,
    PRVDR_NPI_NUM,
    PRVDR_ENRLMT_1ST_NAME,
    PRVDR_ENRLMT_LAST_NAME,
    PRVDR_ENRLMT_MDL_NAME,
    PRVDR_ENRLMT_MDCL_SCHL_ID,
    PRVDR_ENRLMT_GRDTN_YR_NUM,
    ENRLMT.PRVDR_ENRLMT_ID,
    PRVDR_ENRLMT_MLG_LINE_1_ADR,
    PRVDR_ENRLMT_MLG_LINE_2_ADR,
    PRVDR_ENRLMT_INVLD_STATE_CD,
    PRVDR_ENRLMT_INVLD_ZIP_CD,
    PRVDR_ENRLMT_PHNE_NUM,
    PRVDR_ENRLMT_FAX_NUM,
    PRVDR_ENRLMT_EMAIL_ADR
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_MDCR_PRVDR_ENRLMT AS ENRLMT
JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_NPI_CRNT AS NPI_ENRLMT ON 
    NPI_ENRLMT.PRVDR_ENRLMT_ID =
    ENRLMT.PRVDR_ENRLMT_ID
WHERE PRVDR_ENRLMT_TIN_NUM IS NULL OR PRVDR_ENRLMT_TIN_NUM = '~'


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