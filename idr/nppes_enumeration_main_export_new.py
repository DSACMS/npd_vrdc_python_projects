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

# Import the IDROutputter base class
from IDROutputter import IDROutputter


class NPPESMainExporter(IDROutputter):
    """
    Exports NPPES main demographics data using IDROutputter base class.
    
    Handles PII export configuration and pepper-based hashing for sensitive data.
    """
    
    # Class properties
    version_number: str = "v01"
    is_export_pii: bool = False  # Set to True to export PII data
    
    def __init__(self):
        # pepper should be defined in the notebook environment for security
        # DO NOT DEFINE IT IN THIS FILE!
        self.local_pepper = pepper  # This should have a warning as pepper should be external
        
        # Set PII-related properties based on export flag
        if self.is_export_pii:
            self.pii_col_content = 'File_Contains_Provider_PII'
            self.pii_comment_out = ''  # no SQL comment so we get PII columns
        else:
            self.pii_col_content = 'Provider_PII_obscured'
            self.pii_comment_out = '--'  # SQL comment filters out PII
    
    def getSelectQuery(self) -> str:
        """
        Returns the SELECT query for NPPES main export with conditional PII handling.
        
        Uses pepper for hashing sensitive data and conditionally includes/excludes
        PII fields based on the is_export_pii flag.
        """
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
                -- pepper is not defined in this script but in a different snowflake notebook for security. DO NOT DEFINE IT IN THIS FILE! 

                CASE WHEN PRVDR_EIN_NUM IS NOT NULL THEN 
                    SHA2('{self.local_pepper}' || PRVDR_EIN_NUM, 512)
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
                    SHA2('{self.local_pepper}' || PRVDR_PRNT_ORG_TIN_NUM, 512)
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
    
    def do_idr_output(self, *, file_name_stub: str) -> None:
        """
        Override to include PII status in filename.
        """
        # Create filename that includes PII content indicator
        enhanced_filename = f"{file_name_stub}.{self.pii_col_content}"
        
        # Call parent implementation with enhanced filename
        super().do_idr_output(file_name_stub=enhanced_filename)


# Execute the export using the IDROutputter framework
exporter = NPPESMainExporter()
exporter.do_idr_output(file_name_stub="nppes_main_idr_export")

# To download use: 
# snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# Or look in ../misc_scripts/ for misc_scripts/download_and_merge_all_snowflake_csv.sh which downloads the data from idr and then re-merges the csv files.
