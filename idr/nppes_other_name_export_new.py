"""
==========
NPPES Other Name Full Export (IDROutputter Implementation)
==========

Extracts a full export of NPPES other name data including names, roles, contact information,
and historical tracking information with temporal fields.

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class NPPESOtherNameExporter(IDROutputter):
    """
    Exports NPPES other name full data using IDROutputter base class.
    
    Extracts complete NPPES other name records including names, roles, 
    contact information, and historical tracking with temporal fields.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "nppes_othername_with_history"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for NPPES other name full export.
        
        Retrieves complete other name data including temporal tracking
        and derived fields like domain names and current status.
        """
        return """
    SELECT 
        PRVDR_NPI_NUM,
        NAME_ROLE_CD,
        NAME_ROLE_DESC,
        PRVDR_PREX_NAME,
        PRVDR_1ST_NAME,
        PRVDR_MDL_NAME,
        PRVDR_LAST_NAME,
        PRVDR_SFX_NAME,
        ORG_NAME,
        CRDNTL_TXT,
        TITLE_NAME,
        PHNE_NUM,
        PHNE_EXTNSN_NUM,
        EMAIL_ADR,
        SUBSTR(EMAIL_ADR, POSITION('@' IN EMAIL_ADR) + 1) AS domain_name,
        CASE WHEN YEAR(IDR_TRANS_OBSLT_TS) > 3000 THEN 1 ELSE 0 END AS is_current,
        YEAR(IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_YEAR,
        MONTH(IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_MONTH,
        YEAR(IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_YEAR,
        MONTH(IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_MONTH
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_OTHR_NAME_HSTRY
        """


# Execute the export using the IDROutputter framework
exporter = NPPESOtherNameExporter()
exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
