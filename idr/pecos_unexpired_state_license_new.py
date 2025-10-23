"""
==========
PECOS Unexpired Provider State Licenses Per NPI (IDROutputter Implementation)
==========

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
try:
	from IDROutputter import IDROutputter
except ImportError:
     print("Loading IDROutputter from previous cell")


class PecosUnexpiredStateLicenseExporter(IDROutputter):
    """
    Exports PECOS unexpired state license data using IDROutputter base class.
    
    Retrieves current provider licenses that have not yet expired,
    including license numbers, state codes, and associated specialty information.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "pecos_unexpired_license"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for PECOS unexpired state license export.
        
        Retrieves current licenses with expiration dates beyond last year,
        joined with NPI and specialty information.
        
        Date calculation is done in SQL to avoid datetime dependencies at import time.
        """
        return """
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
    WHERE YEAR(PRVDR_ENRLMT_LCNS_EXPRTN_DT) > (YEAR(CURRENT_DATE()) - 1)
        """


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    exporter = PecosUnexpiredStateLicenseExporter()
    exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
