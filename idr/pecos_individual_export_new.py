"""
==========
PECOS Individual Export (IDROutputter Implementation)
==========

Exports the PAC_IDs and the NPIs so that we can leverage the PAC_IDs as a mechanism to represent organizations that is already public.
See: 

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class PecosIndividualExporter(IDROutputter):
    """
    Exports PECOS individual provider data using IDROutputter base class.
    
    Exports PAC_IDs and NPIs for individual providers to leverage PAC_IDs
    as a public mechanism to represent organizations.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "pecos_individual_export"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for PECOS individual export.
        
        Retrieves PAC_IDs, NPIs, and provider information for individuals
        (providers without TIN numbers or with placeholder TIN values).
        """
        return """
SELECT 
    PRVDR_PAC_ID,
    PRVDR_NPI_NUM,
    PRVDR_ENRLMT_1ST_NAME,
    PRVDR_ENRLMT_LAST_NAME,
    PRVDR_ENRLMT_MDL_NAME,
    PRVDR_ENRLMT_MDCL_SCHL_ID,
    PRVDR_ENRLMT_GRDTN_YR_NUM,
    ENRLMT.PRVDR_ENRLMT_ID,
    PRVDR_ENRLMT_MLG_LINE_1_ADR,
    PRVDR_ENRLMT_MLG_LINE_2_ADR,
    PRVDR_ENRLMT_INVLD_STATE_CD,
    PRVDR_ENRLMT_INVLD_ZIP_CD,
    PRVDR_ENRLMT_PHNE_NUM,
    PRVDR_ENRLMT_FAX_NUM,
    PRVDR_ENRLMT_EMAIL_ADR
FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_MDCR_PRVDR_ENRLMT AS ENRLMT
JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_NPI_CRNT AS NPI_ENRLMT ON 
    NPI_ENRLMT.PRVDR_ENRLMT_ID =
    ENRLMT.PRVDR_ENRLMT_ID
WHERE PRVDR_ENRLMT_TIN_NUM IS NULL OR PRVDR_ENRLMT_TIN_NUM = '~'
        """


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    exporter = PecosIndividualExporter()
    exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
