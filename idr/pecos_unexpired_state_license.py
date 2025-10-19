"""
PECOS Unexpired State Licenses Per NPI
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

license_file_name = f"@~/pecos_unexpired_license.{ts}.csv"


now = datetime.now()
last_year = now.year - 1 


license_sql = f"""
COPY INTO {license_file_name}
FROM (
SELECT
    PRVDR_NPI_NUM,
    PRVDR_ENRLMT_LCNS_STATE_CD,
    PRVDR_ENRLMT_LCNS_NUM, 
    PRVDR_ENRLMT_FORM_CD,
    PRVDR_ENRLMT_SPCLTY_CD,
    PRVDR_ENRLMT_SPCLTY_DESC
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_LCNS_CRNT AS current_license
JOIN  IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_NPI_CRNT enrollment_to_npi ON
  enrollment_to_npi.prvdr_enrlmt_id =
  current_license.prvdr_enrlmt_id
JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_SPCLTY_CRNT AS enrollment_speciality ON 
    enrollment_speciality.prvdr_enrlmt_id =
    enrollment_to_npi.prvdr_enrlmt_id
WHERE YEAR(PRVDR_ENRLMT_LCNS_EXPRTN_DT) > {last_year}
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

session.sql(license_sql).collect()


# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 