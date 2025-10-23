"""
==========
Medicaid Provider Licensing Credential Export (IDROutputter Implementation)
==========


This script downloads Medicaid provider credential data, focusing on licenses and license numbers. It performs the following tasks:
	•	Retrieves the list of licenses associated with each provider.
	•	Extracts key fields including:
	•	License type code
	•	License type description
	•	License number
	•	Resolves license type codes using a lookup table.
	•	Limits results to providers with valid state-level Medicare codes.
	•	Excludes license entries that are clearly not useful — for example, records that only restate the NPI instead of a real license.

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""

# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class MedicaidProviderCredentialsExporter(IDROutputter):
    """
    Exports Medicaid provider credential data using IDROutputter base class.
    
    Downloads Medicaid provider credential data focusing on licenses and license numbers.
    Resolves license type codes and excludes non-useful license entries.
    """
    
    # Class properties  
    version_number: str = "v01"
    file_name_stub: str = "medicaid_provider_credentials"
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for Medicaid provider credentials export.
        
        Retrieves license information with type descriptions, filtering for valid
        state-level Medicare codes and excluding non-useful license entries.
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


# Execute the export using the IDROutputter framework
exporter = MedicaidProviderCredentialsExporter()
exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
