
"""
Current Demographics table export for IDR NPPES. Contains most of the main file along with some inside-cms only data. 

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

medicaid_credentials_file_name = f"@~/medicaid_credentials.{ts}.csv"

medicaid_credentials_sql = f"""
COPY INTO {medicaid_credentials_file_name}""" + """
FROM (
            SELECT
                PRVDR_NPI_NUM,
                PRVDR_ENT_TYPE_CD,
                PRVDR_ENT_TYPE_DESC,
                PRVDR_CRTFCTN_DT,
                PRVDR_RPLCMT_NPI_NUM,
                PRVDR_SSN_NUM, -- PII
                PRVDR_TIN_NUM, -- possible PII when not null
                PRVDR_EIN_NUM,
                PRVDR_ENMRTN_DT,
                PRVDR_LAST_UPDT_DT,
                PRVDR_DEACTVTN_RSN_CD,
                PRVDR_DEACTVTN_RSN_DESC,
                PRVDR_DEACTVTN_DT,
                PRVDR_REACTVTN_DT,
                PRVDR_GNDR_CD,
                PRVDR_BIRTH_USPS_STATE_CD,
                PRVDR_BIRTH_CNTRY_CD,
                PRVDR_SOLE_PRPRTR_CD,
                PRVDR_ORG_SUBRDNT_CD,
                PRVDR_PRNT_ORG_TIN_NUM,
                PRVDR_PRMRY_LANG_TXT, -- greenlocked!!! 
                PRVDR_PREX_NAME,
                PRVDR_1ST_NAME,
                PRVDR_MDL_NAME,
                PRVDR_LAST_NAME,
                PRVDR_SFX_NAME,
                ORG_NAME,
                MLG_LINE_1_ADR,
                MLG_LINE_2_ADR,
                MLG_ADR_CITY_NAME,
                MLG_GEO_USPS_STATE_CD,
                MLG_GEO_ZIP4_CD,
                MLG_GEO_ZIP5_CD,
                MLG_CNTRY_CD,
                MLG_PHNE_NUM,
                MLG_PHNE_EXTNSN_NUM,
                MLG_FAX_NUM,
                PRCTC_LINE_1_ADR,
                PRCTC_LINE_2_ADR,
                PRCTC_ADR_CITY_NAME,
                PRCTC_GEO_USPS_STATE_CD,
                PRCTC_GEO_ZIP4_CD,
                PRCTC_GEO_ZIP5_CD,
                PRCTC_CNTRY_CD,
                PRCTC_PHNE_NUM,
                PRCTC_PHNE_EXTNSN_NUM,
                PRCTC_FAX_NUM
                

            FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_DMGRPHCS_CRNT
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