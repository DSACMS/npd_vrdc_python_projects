"""
==========
NPPES Historical Address Export (IDROutputter Implementation)
==========


This script downloads historical NPPES provider street address information on a per-NPI basis. 

The data it retrieves includes:
	•	Street address details
	•	Phone numbers
	•	Fax numbers
	•	The year and month when the address was first entered into NPPES
	•	The year and month when the address was deleted or removed from NPPES

Day-level precision is intentionally excluded. We only retain year and month to avoid inadvertently creating a linkable dataset using exact dates; 
month-level granularity is sufficient to track when addresses change without increasing data-linking risk.

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class NPPESHistoricalAddressExporter(IDROutputter):
    """
    Exports NPPES historical address data using IDROutputter base class.
    
    Downloads historical NPPES provider street address information for NPIs
    that have had multiple practice addresses, with year/month granularity
    for privacy protection.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "nppes_old_addresses"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for NPPES historical address export.
        
        Retrieves address history for NPIs with multiple practice addresses,
        including temporal information at year/month granularity.
        """
        return """
        SELECT 
            address_history_outside.PRVDR_NPI_NUM,
            address_history_outside.LINE_1_ADR,
            address_history_outside.LINE_2_ADR,
            address_history_outside.ADR_CITY_NAME,
            address_history_outside.GEO_USPS_STATE_CD,
            address_history_outside.ZIP5_CD,
            address_history_outside.ZIP4_CD,
            address_history_outside.CNTRY_CD,
            address_history_outside.PHNE_NUM,
            address_history_outside.FAX_NUM,
            YEAR(address_history_outside.IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_YEAR,
            MONTH(address_history_outside.IDR_TRANS_OBSLT_TS) AS ADDRESS_TO_MONTH,
            YEAR(address_history_outside.IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_YEAR,
            MONTH(address_history_outside.IDR_TRANS_EFCTV_TS) AS ADDRESS_FROM_MONTH
        FROM (
            SELECT PRVDR_NPI_NUM 
            FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY
            WHERE ADR_TYPE_CD = '03'
            GROUP BY PRVDR_NPI_NUM
            HAVING COUNT(*) > 1
        ) AS npis_with_at_least_two_practice_addresses
        LEFT JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_ADR_HSTRY AS address_history_outside ON (
            address_history_outside.PRVDR_NPI_NUM = 
            npis_with_at_least_two_practice_addresses.PRVDR_NPI_NUM
            AND 
            address_history_outside.ADR_TYPE_CD = '03'
            )
        WHERE YEAR(address_history_outside.IDR_TRANS_OBSLT_TS) < 9000
        ORDER BY address_history_outside.PRVDR_NPI_NUM DESC
        """


# Execute the export using the IDROutputter framework
exporter = NPPESHistoricalAddressExporter()
exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
