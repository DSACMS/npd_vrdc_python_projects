"""
==========
Medicaid Provider Service Address Export
==========

Simple export of TMSIS address data, and associated NPIs when they occur for more than a given threshold
of patients over a time period. Because address locations are particularly sensitive when they are NOT
actual hospital/clinic/snf/etc locations, we are requiring that this threshold be pretty high: 50 patients at least 
in the initial implementation. This might lower all the way to CMS standard 11 but for now we will keep it high.
Also we are looking for the most common service locations in any case.. it is not obvious that 
service locations which low patient volume have the same stability or usefulness. 

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

medicaid_service_locations_file_name = f"@~/medicaid_provider_service_locations.{ts}.csv"

patient_aggregation_threshold = 50

medicaid_service_locations_sql = f"""
COPY INTO {medicaid_service_locations_file_name}
FROM (
	SELECT 
		CLM_RX_DSPNSNG_PRVDR_NPI_NUM,
		CLM_SPRVSNG_PRVDR_NPI_NUM,
		CLM_SRVC_LCTN_ORG_NPI_NUM,
		CLM_LINE_SRVCNG_PRVDR_NPI_NUM,
		CLM_LINE_SRVC_LCTN_ORG_NPI_NUM,
		CLM_LINE_SRVC_LCTN_LINE_1_ADR,
		CLM_LINE_SRVC_LCTN_LINE_2_ADR,
		CLM_LINE_SRVC_LCTN_CITY_NAME,
		CLM_LINE_SRVC_LCTN_STATE_CD,
		CLM_LINE_SRVC_LCTN_ZIP_CD,
		COUNT(DISTINCT(CLM_MBI_NUM)) AS count_CLM_MBI_NUM
	FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM AS CLM -- Provider data usually lives here.
	JOIN IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_CLM_LINE AS CLINE ON -- address data lives here..
		CLINE.CLM_UNIQ_ID = CLM.CLM_UNIQ_ID  
	WHERE 
    	YEAR(CLM.CLM_FROM_DT) = 2025
		AND CLM_LINE_SRVC_LCTN_LINE_1_ADR IS NOT NULL -- lots of times in this data structure there will be no address at all
		AND CLM_LINE_SRVC_LCTN_CITY_NAME IS NOT NULL -- but if these two are set, then there tend not to be NULL values in the other address fields. 
	GROUP BY    -- by all of the columns essentiall.
		CLM_RX_DSPNSNG_PRVDR_NPI_NUM,
		CLM_SPRVSNG_PRVDR_NPI_NUM,
		CLM_SRVC_LCTN_ORG_NPI_NUM,
		CLM_LINE_SRVCNG_PRVDR_NPI_NUM,
		CLM_LINE_SRVC_LCTN_ORG_NPI_NUM,
		CLM_LINE_SRVC_LCTN_LINE_1_ADR,
		CLM_LINE_SRVC_LCTN_LINE_2_ADR,
		CLM_LINE_SRVC_LCTN_CITY_NAME,
		CLM_LINE_SRVC_LCTN_STATE_CD,
		CLM_LINE_SRVC_LCTN_ZIP_CD
	HAVING COUNT(DISTINCT(CLM_MBI_NUM)) > {patient_aggregation_threshold} -- a very high patient privacy threshold. This is the number of patients who must have been seen at this location to be in the data.

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

session.sql(medicaid_service_locations_sql).collect()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../idr_data/ for idr_data/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files. 
