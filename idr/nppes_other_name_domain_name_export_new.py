"""
==========
NPPES Other Name Domain Name Export (IDROutputter Implementation)
==========

Extracts a list of the common domain names associated with provider email addresses, where that field is not null.

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class NPPESOtherNameDomainExporter(IDROutputter):
    """
    Exports NPPES other name email domain analysis using IDROutputter base class.
    
    Extracts and analyzes the common domain names from provider email addresses
    with counts and percentages.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "nppes_contact_email_domainname"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for NPPES email domain name analysis.
        
        Analyzes email domains from NPPES other name data, calculating
        NPI counts and percentages for each domain.
        """
        return """
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
        """


# Execute the export using the IDROutputter framework
exporter = NPPESOtherNameDomainExporter()
exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
