"""
==========
PECOS Current Provider Practice Addresses Per NPI (IDROutputter Implementation)
==========

This file contains two related exports:
1. Full recent practice address data 
2. Summary address data

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class PecosRecentPracticeAddressExporter(IDROutputter):
    """
    Exports PECOS recent practice address data using IDROutputter base class.
    
    Retrieves current practice addresses (where obsolete year = 9999) 
    that were effective after 2022.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "pecos_recent_practice_address"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for PECOS recent practice address export.
        
        Retrieves current practice addresses with temporal filtering
        and year-only granularity for privacy.
        """
        return """
    SELECT 
        PRVDR_NPI_NUM,
        ADR_TYPE_DESC,
        LINE_1_ADR,
        LINE_2_ADR,
        GEO_USPS_STATE_CD,
        ADR_CITY_NAME,
        ZIP5_CD,
        ZIP4_CD,
        CNTRY_CD,
        PHNE_NUM,
        FAX_NUM,
        YEAR(IDR_UPDT_TS) AS IDR_UPDT_TS_YEAR,
        YEAR(IDR_TRANS_OBSLT_TS) AS IDR_TRANS_OBSLT_TS_YEAR,
        YEAR(IDR_TRANS_EFCTV_TS) AS IDR_TRANS_EFCTV_TS_YEAR
    FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
    WHERE YEAR(IDR_TRANS_OBSLT_TS) = 9999 AND YEAR(IDR_TRANS_EFCTV_TS) > 2022 AND ADR_TYPE_DESC = 'PRACTICE'
        """


class PecosAddressSummaryExporter(IDROutputter):
    """
    Exports PECOS address summary data using IDROutputter base class.
    
    Provides a simplified summary of current practice addresses
    with just key location fields.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "pecos_address_summary"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for PECOS address summary export.
        
        Retrieves summarized address information for current practice addresses.
        """
        return """
SELECT 
    PRVDR_NPI_NUM,
    GEO_USPS_STATE_CD,
    ADR_CITY_NAME,
    ZIP5_CD,
    ZIP4_CD
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
WHERE YEAR(IDR_TRANS_OBSLT_TS) = 9999 AND YEAR(IDR_TRANS_EFCTV_TS) > 2022 AND ADR_TYPE_DESC = 'PRACTICE'
        """


# Execute both exports using the IDROutputter framework
print("Exporting PECOS recent practice address data...")
recent_exporter = PecosRecentPracticeAddressExporter()
recent_exporter.do_idr_output()

print("Exporting PECOS address summary data...")
summary_exporter = PecosAddressSummaryExporter()
summary_exporter.do_idr_output()

print("Both exports completed!")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
