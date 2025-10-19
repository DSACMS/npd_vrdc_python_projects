
"""
Current Demographics table export for IDR NPPES. Contains most of the main file along with some inside-cms only data. 

Methods applied to obscure covert pii into exportable data. 

Uses the same salt as the tinhash etc. 

is_export_pii is set to False for now. As NPPES replacement approaches we will need to change this decision

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



is_export_pii = False

if(is_export_pii):
    pii_col_content = 'File_Contains_Provider_PII'
    pii_comment_out = '' # no SQL comment so we are getting this column of data
else:
    pii_col_content = 'Provider_PII_obscured'
    pii_comment_out = '--' # SQL comment filters out problematic pii

nppes_IDR_main_export_filename = f"@~/medicaid_credentials.{pii_col_content}.{ts}.csv"

nppes_IDR_main_export_sql = f"""
COPY INTO {nppes_IDR_main_export_filename}""" + """
FROM (
            SELECT
                '{pii_col_content}' AS does_file_contain_pii
                PRVDR_NPI_NUM,
                PRVDR_ENT_TYPE_CD,
                PRVDR_ENT_TYPE_DESC,
                PRVDR_CRTFCTN_DT,
                PRVDR_RPLCMT_NPI_NUM,

                {pii_comment_out} PRVDR_SSN_NUM, --pii
                {pii_comment_out} PRVDR_TIN_NUM, --pii
                {pii_comment_out} PRVDR_EIN_NUM, --possible pii
                -- salt is not defined in this script but in a different snowflake notebook for security. DO NOT DEFINE IT IN THIS FILE! 
                SHA2('{salt}' || PRVDR_SSN_NUM, 512) AS PRVDR_SSN_NUM_salted_hash_sha512 ,
                SHA2('{salt}' || PRVDR_TIN_NUM, 512) AS PRVDR_TIN_NUM_salted_hash_sha512 , -- rarely used.
                SHA2('{salt}' || PRVDR_EIN_NUM, 512) AS PRVDR_EIN_NUM_salted_hash_sha512 ,

                PRVDR_ENMRTN_DT,
                PRVDR_LAST_UPDT_DT,
                PRVDR_DEACTVTN_RSN_CD,
                PRVDR_DEACTVTN_RSN_DESC,
                PRVDR_DEACTVTN_DT,
                PRVDR_REACTVTN_DT,
                PRVDR_GNDR_CD,

                {pii_comment_out} PRVDR_BIRTH_USPS_STATE_CD, -- pii 
                {pii_comment_out} PRVDR_BIRTH_CNTRY_CD, -- pii
                {pii_comment_out} PRVDR_BIRTH_DT, -- pii
                CONCAT(
                        FLOOR(PRVDR_BIRTH_DT / 5) * 5,
                            '-',
                            FLOOR(PRVDR_BIRTH_DT / 5) * 5 + 4
                ) AS PRVDR_BIRTH_DT_five_year_birth_span -- blurred indication of general provider age. 

                PRVDR_SOLE_PRPRTR_CD,

                PRVDR_ORG_SUBRDNT_CD,
                {pii_comment_out} PRVDR_PRNT_ORG_TIN_NUM, --possible pii
                SHA2('{salt}' || PRVDR_PRNT_ORG_TIN_NUM, 512) AS PRVDR_PRNT_ORG_TIN_NUM_salted_hash_sha512

                {pii_comment_out} PRVDR_PRMRY_LANG_TXT, -- pii which is further greenlocked!!! 

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

session.sql(nppes_IDR_main_export_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 