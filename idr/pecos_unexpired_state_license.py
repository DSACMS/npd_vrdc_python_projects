# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import streamlit as st
import pandas as pd
from datetime import datetime

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

now = datetime.now()
last_year = now.year - 1 


address_sql = f"""
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

"""

df = session.sql(address_sql).to_pandas()

ts = datetime.now().strftime("%Y_%m_%d_%H%M")

address_file_name = f"pecos_recent_practice_address.{ts}.csv"

session.file.put_stream(
    df.to_csv(address_file_name, index = False),
    f"@~/{address_file_name}",
    auto_compress=False
)

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"