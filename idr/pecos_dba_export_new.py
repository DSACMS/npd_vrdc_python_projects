"""
==========
PECOS DBA Name History (IDROutputter Implementation)
==========

Note that a LEFT JOIN here does return more results.. 
but I am not sure what that means, so I am keeping the cleaner INNER JOIN.
TODO: figure out why a LEFT JOIN has more results and figure out what that means. 

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd
from datetime import datetime

# Import the IDROutputter base class
try:
	from IDROutputter import IDROutputter
except ImportError:
     print("Loading IDROutputter from previous cell")


class PecosDbaExporter(IDROutputter):
    """
    Exports PECOS DBA name history using IDROutputter base class.
    
    Retrieves historical DBA (Doing Business As) names for providers
    with enrollment history data.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "pecos_dba_history"
    
    def __init__(self):
        # Calculate date values that might be used for filtering in future versions
        now = datetime.now()
        self.last_year = now.year - 1
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for PECOS DBA name history export.
        
        Retrieves DBA name history with enrollment associations using
        INNER JOIN (cleaner than LEFT JOIN based on analysis in original file).
        """
        return """
  SELECT 
    PRVDR_NPI_NUM,
    dba_history.PRVDR_ORG_NAME,
    enrollment_to_npi.IDR_TRANS_EFCTV_TS AS enrollment_to_npi_IDR_TRANS_EFCTV_TS,
    enrollment_to_npi.IDR_TRANS_OBSLT_TS AS enrollment_to_npi_IDR_TRANS_OBSLT_TS,
    dba_history.PRVDR_ENRLMT_ID,
    dba_history.PRVDR_PAC_ID,
    dba_history.PRVDR_ENRLMT_ASCTN_ROLE_CD,
    dba_history.PRVDR_ENRLMT_ASCTN_ROLE_DESC,
    dba_history.IDR_TRANS_EFCTV_TS AS dba_history_IDR_TRANS_EFCTV_TS,
    dba_history.IDR_TRANS_OBSLT_TS AS dba_history_IDR_TRANS_OBSLT_TS


  FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_ASCTN_DBA_NAME_HSTRY  AS dba_history
      JOIN  IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENRLMT_NPI_HSTRY AS enrollment_to_npi ON
        enrollment_to_npi.prvdr_enrlmt_id =
        dba_history.prvdr_enrlmt_id
        """


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    exporter = PecosDbaExporter()
    exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
