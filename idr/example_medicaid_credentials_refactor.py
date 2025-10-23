"""
==========
Example: Medicaid Provider Credentials Export using IDROutputter
==========

This demonstrates how to refactor medicaid_provider_credentials.py to use the new IDROutputter class.
This example shows the "tail" of the import file instantiating the class and calling do_idr_output().
"""

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class MedicaidProviderCredentialsExporter(IDROutputter):
    """
    Exports Medicaid provider credential data using IDROutputter base class.
    """
    
    # Class property - version number for filename
    version_number: str = "v01"
    
    def getSelectQuery(self) -> str:
        """
        Must-override method that returns the SELECT query string for Medicaid credentials.
        """
        return """
		SELECT 
			PRVDR_STATE_MDCD_ID,
			PRVDR_LCNS_ISSG_ENT_ID,
			PRVDR_LCNS_OR_ACRDTN_NUM,
			license_list.PRVDR_MDCD_LCNS_TYPE_CD,
			PRVDR_MDCD_LCNS_TYPE_CD_DESC,
			PRVDR_LCNS_OR_ACRDTN_NUM
		
		FROM IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_PRVDR_LCNS_CRNT AS license_list 
		JOIN IDRC_PRD.CMS_VDM_VIEW_MDCD_PRD.V2_MDCD_PRVDR_LCNS_TYPE_CD AS license_type ON 
			license_type.PRVDR_MDCD_LCNS_TYPE_CD =
			license_list.PRVDR_MDCD_LCNS_TYPE_CD
		WHERE  REGEXP_LIKE(PRVDR_STATE_MDCD_ID,'^[1][0-9]{9}$') 
		AND license_list.PRVDR_MDCD_LCNS_TYPE_CD != '2' 
		AND license_list.PRVDR_MDCD_LCNS_TYPE_CD != '~'
		AND PRVDR_LCNS_ISSG_ENT_ID != 'NPI'
        """


# The "tail" of the import file - instantiate the class and call do_idr_output() method
exporter = MedicaidProviderCredentialsExporter()
exporter.do_idr_output(file_name_stub="medicaid_provider_credentials")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh
