"""
==========
Medicaid Provider Licensing Credential Export
==========


This script downloads Medicaid provider credential data, focusing on licenses and license numbers. It performs the following tasks:
	•	Retrieves the list of licenses associated with each provider.
	•	Extracts key fields including:
	•	License type code
	•	License type description
	•	License number
	•	Resolves license type codes using a lookup table.
	•	Limits results to providers with valid state-level Medicare codes.
	•	Excludes license entries that are clearly not useful — for example, records that only restate the NPI instead of a real license.

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
COPY INTO {medicaid_credentials_file_name}
FROM (
		SELECT 
			PRVDR_STATE_MDCD_ID,
			PRVDR_LCNS_ISSG_ENT_ID,
			PRVDR_LCNS_OR_ACRDTN_NUM,
			license_list.PRVDR_MDCD_LCNS_TYPE_CD,
			PRVDR_MDCD_LCNS_TYPE_CD_DESC,
			PRVDR_LCNS_OR_ACRDTN_NUM
		
		FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_PRVDR_LCNS_CRNT AS license_list 
		JOIN IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_PRVDR_LCNS_TYPE_CD AS license_type ON 
			license_type.PRVDR_MDCD_LCNS_TYPE_CD =
			license_list.PRVDR_MDCD_LCNS_TYPE_CD
		WHERE  REGEXP_LIKE(PRVDR_STATE_MDCD_ID,'^[1][0-9]{9}$') 
		AND license_list.PRVDR_MDCD_LCNS_TYPE_CD != '2' 
		AND license_list.PRVDR_MDCD_LCNS_TYPE_CD != '~'
		AND PRVDR_LCNS_ISSG_ENT_ID != 'NPI'
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

session.sql(medicaid_credentials_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 
