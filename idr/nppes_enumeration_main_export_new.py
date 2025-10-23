"""
==========
NPPES MAIN IDR Export (IDROutputter Implementation)
==========


Current Demographics table export for IDR NPPES. Contains most of the main file along with some inside-cms only data. 

Methods applied to obscure covert pii into exportable data. 

Uses the same pepper as the tinhash etc. 

is_export_pii is set to False for now. As NPPES replacement approaches we will need to change this decision

This is the NEW implementation using the IDROutputter base class to eliminate code duplication.
"""
# Note: you must have an appropriate role chosen and the IDRC_PRD_COMM_WH warehouse selected

# Import python packages
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session  # type: ignore

# Import the IDROutputter base class
try:
	from IDROutputter import IDROutputter
except ImportError:
     print("Loading IDROutputter from previous cell")


class NPPESMainExporter(IDROutputter):
    """
    Exports NPPES main demographics data using IDROutputter base class.
    
    Handles PII export configuration and pepper-based hashing for sensitive data.
    """
    
    # Class properties
    version_number: str = "v01"
    file_name_stub: str = "nppes_main_idr_export"
    is_export_pii: bool = False  # Set to True to export PII data
    
    def __init__(self):
        # Initialize pepper as None - must be set via setPepper() before database operations
        self.local_pepper = None
        
        # Set PII-related properties based on export flag
        if self.is_export_pii:
            self.pii_col_content = 'File_Contains_Provider_PII'
            self.pii_comment_out = ''  # no SQL comment so we get PII columns
        else:
            self.pii_col_content = 'Provider_PII_obscured'
            self.pii_comment_out = '--'  # SQL comment filters out PII
    
    def setPepper(self, pepper_value: str) -> None:
        """
        Set the pepper value for hashing operations.
        
        This method must be called before do_idr_output() to set the pepper
        used for sensitive data hashing. For security, the pepper should come
        from the notebook environment, not be defined in code.
        
        Args:
            pepper_value (str): The pepper string to use for hashing
        """
        self.local_pepper = pepper_value
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for NPPES main export with conditional PII handling.
        
        Uses pepper for hashing sensitive data and conditionally includes/excludes
        PII fields based on the is_export_pii flag.
        
        If pepper hasn't been set via setPepper(), uses 'temp_pepper' as default
        for SQL generation purposes.
        """
        # Use actual pepper if set, otherwise use temp placeholder for query generation
        pepper_to_use = self.local_pepper if self.local_pepper is not None else 'temp_pepper'
        
        return f"""
            SELECT
                '{self.pii_col_content}' AS does_file_contain_pii,
                PRVDR_NPI_NUM,
                PRVDR_ENT_TYPE_CD,
                PRVDR_ENT_TYPE_DESC,
                PRVDR_CRTFCTN_DT,
                PRVDR_RPLCMT_NPI_NUM,

                {self.pii_comment_out} PRVDR_SSN_NUM, --pii
                {self.pii_comment_out} PRVDR_TIN_NUM, --pii
                PRVDR_EIN_NUM, 
                -- pepper is set via setPepper() method for security. Uses temp_pepper for query generation if not set.

                CASE WHEN PRVDR_EIN_NUM IS NOT NULL THEN 
                    SHA2('{pepper_to_use}' || PRVDR_EIN_NUM, 512)
                ELSE NULL END AS PRVDR_EIN_NUM_peppered_hash_sha512,

                PRVDR_ENMRTN_DT,
                PRVDR_LAST_UPDT_DT,
                PRVDR_DEACTVTN_RSN_CD,
                PRVDR_DEACTVTN_RSN_DESC,
                PRVDR_DEACTVTN_DT,
                PRVDR_REACTVTN_DT,
                PRVDR_GNDR_CD,

                {self.pii_comment_out} PRVDR_BIRTH_USPS_STATE_CD, -- pii 
                {self.pii_comment_out} PRVDR_BIRTH_CNTRY_CD, -- pii
                {self.pii_comment_out} PRVDR_BIRTH_DT, -- pii
                CONCAT(  
                            FLOOR(YEAR(PRVDR_BIRTH_DT) / 5) * 5,  
                            '-',  
                            FLOOR(YEAR(PRVDR_BIRTH_DT) / 5) * 5 + 4  
                ) AS PRVDR_BIRTH_DT_five_year_birth_span, -- blurred indication of general provider age. 

                PRVDR_SOLE_PRPRTR_CD,

                PRVDR_ORG_SUBRDNT_CD,
                PRVDR_PRNT_ORG_TIN_NUM, --possible pii
                

                CASE WHEN PRVDR_PRNT_ORG_TIN_NUM IS NOT NULL THEN 
                    SHA2('{pepper_to_use}' || PRVDR_PRNT_ORG_TIN_NUM, 512)
                ELSE NULL END AS PRVDR_PRNT_ORG_TIN_NUM_peppered_hash_sha512,

                {self.pii_comment_out} PRVDR_PRMRY_LANG_TXT, -- pii which is further greenlocked!!! 

                PRVDR_PREX_NAME,
                PRVDR_1ST_NAME,
                PRVDR_MDL_NAME,
                PRVDR_LAST_NAME,
                PRVDR_SFX_NAME,
                ORG_NAME,
                MLG_LINE_1_ADR,
                MLG_LINE_2_ADR,
                MLG_ADR_CITY_NAME,
                MLG_GEO_USPS_STATE_CD,
                MLG_GEO_ZIP4_CD,
                MLG_GEO_ZIP5_CD,
                MLG_CNTRY_CD,
                MLG_PHNE_NUM,
                MLG_PHNE_EXTNSN_NUM,
                MLG_FAX_NUM,
                PRCTC_LINE_1_ADR,
                PRCTC_LINE_2_ADR,
                PRCTC_ADR_CITY_NAME,
                PRCTC_GEO_USPS_STATE_CD,
                PRCTC_GEO_ZIP4_CD,
                PRCTC_GEO_ZIP5_CD,
                PRCTC_CNTRY_CD,
                PRCTC_PHNE_NUM,
                PRCTC_PHNE_EXTNSN_NUM,
                PRCTC_FAX_NUM
                

            FROM IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_PRVDR_ENMRT_DMGRPHCS_CRNT
        """
    
    def pre_export_validation(self) -> None:
        """
        Validate that pepper has been set before export.
        Called by the base class before any export operations begin.
        """
        if self.local_pepper is None:
            raise ValueError(
                f"Pepper must be set before running do_idr_output(). "
                f"Call setPepper('your_pepper_value') first. "
                f"The pepper should come from your notebook environment for security."
            )
    
    def get_filename_components(self) -> list[str]:
        """
        Add PII status component to filename.
        Returns PII content to be included in filename between stub and version.
        
        Results in filename format: {file_name_stub}.{pii_content}.{version}.{timestamp}.csv
        """
        return [self.pii_col_content]


if __name__ == '__main__':
    # Execute the export using the IDROutputter framework
    exporter = NPPESMainExporter()
    # Set pepper from notebook environment before running export
    # exporter.setPepper(pepper)  # pepper should be defined in notebook environment - uncomment in notebook
    exporter.do_idr_output()

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
