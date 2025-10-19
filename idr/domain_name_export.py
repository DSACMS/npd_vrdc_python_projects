
"""
==========
NPPES Domain Name Export
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


nppes_contact_email_domainname_filename = f"@~/nppes_contact_email_domainname.{ts}.csv"

nppes_IDR_main_export_sql = f"""
COPY INTO {nppes_contact_email_domainname_filename}""" + f"""
FROM (

    SELECT 
        SUBSTR(EMAIL_ADR, POSITION('@' IN EMAIL_ADR) + 1) AS domain_name,
        COUNT(DISTINCT(PRVDR_NPI_NUM)) AS npi_count,
        (
            (COUNT(DISTINCT(PRVDR_NPI_NUM))/ -- part 
            email_total_join.total_npi_with_email_count) -- whole
            * 100 -- into percent
        ) AS domain_percent
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_OTHR_NAME_HSTRY
    JOIN (
        SELECT COUNT(DISTINCT(PRVDR_NPI_NUM)) AS total_npi_with_email_count
            FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_OTHR_NAME_HSTRY
            WHERE EMAIL_ADR IS NOT NULL
        ) AS email_total_join    
    GROUP BY SUBSTR(EMAIL_ADR, POSITION('@' IN EMAIL_ADR) + 1), total_npi_with_email_count
    ORDER BY npi_count DESC

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