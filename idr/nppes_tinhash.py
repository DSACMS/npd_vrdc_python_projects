"""

This creates a map of the salted + hashed ONPI relationships. 

This program relies on the 'salt' variable being set in a previous notebook. 
not setting this in the scope of this program ensures that we do not accidentally 
record the salt into git. Not quite as bad as a password.. but still bad. 

Under the current model, the salt should be changed every run. But it should be used for all the various EIN Perumations. 

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

nppes_onpi_tin_file_name = f"@~/nppes_onpi_tin.{ts}.csv"

nppes_onpi_tin_sql = f"""
COPY INTO {nppes_onpi_tin_file_name}
FROM (
      SELECT PRVDR_NPI_NUM, 
      PRVDR_ENT_TYPE_CD,
      PRVDR_ORG_SUBRDNT_CD,
      ORG_NAME,
      -- salt is not defined in this script but in a different snowflake notebook for security. DO NOT DEFINE IT IN THIS FILE! 
      SHA2('{salt}' || PRVDR_EIN_NUM, 512) AS tin_salted_hash_sha512,
      FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_DMGRPHCS_CRNT
      WHERE PRVDR_EIN_NUM IS NOT NULL
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

session.sql(nppes_onpi_tin_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 
