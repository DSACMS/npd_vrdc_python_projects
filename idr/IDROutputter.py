"""
==========
IDR Outputter Base Class
==========

This abstract base class provides a standardized way to handle repetitive IDR export scripts.
Each export script follows the same pattern:
1. Create a timestamped CSV filename
2. Wrap a SELECT query in a COPY INTO statement
3. Execute the query and export to Snowflake stage

Usage:
- Subclass IDROutputter
- Implement getSelectQuery() method to return your specific SELECT query
- Set version_number and version class properties
- Call do_idr_output(file_name_stub="your_filename") to execute

File naming format: {file_name_stub}.{version_number}{timestamp}.csv
Example: medicaid_provider_credentials.v01_2024_10_22_2207.csv
"""

from abc import ABC, abstractmethod
from datetime import datetime
from snowflake.snowpark.context import get_active_session  # type: ignore


class IDROutputter(ABC):
    """
    Abstract base class for IDR data export operations.
    
    Provides standardized export functionality for Snowflake COPY INTO operations.
    Each subclass must implement getSelectQuery() to return their specific SELECT query.
    """
    
    # Class properties with defaults
    version_number: str = "v01"
    version: str = "1.0.0"  # File version identifier
    
    @staticmethod
    def execute_idr_export(*, select_query: str, file_name_stub: str, version_number: str) -> None:
        """
        Static method that executes IDR export operations.
        
        Takes a SELECT query and wraps it in a standardized COPY INTO statement,
        then executes it against the active Snowflake session.
        
        Args:
            select_query: The SELECT query to export data from
            file_name_stub: Base filename (without timestamp/extension)  
            version_number: Version identifier to include in filename
            
        Raises:
            Exception: If Snowflake query execution fails
        """
        try:
            session = get_active_session()
            ts = datetime.now().strftime("%Y_%m_%d_%H%M")
            full_filename = f"@~/{file_name_stub}.{version_number}_{ts}.csv"
            
            copy_sql = f"""
COPY INTO {full_filename}
FROM (
{select_query}
)
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = NONE
)
HEADER = TRUE
OVERWRITE = TRUE;
"""
            
            print(f"IDROutputter: Starting export for {file_name_stub}...")
            print(f"IDROutputter: Target file will be {full_filename}")
            
            session.sql(copy_sql).collect()
            
            print(f"IDROutputter: Successfully exported data to {full_filename}")
            print(f"IDROutputter: Export completed for {file_name_stub}")
            
        except Exception as e:
            print(f"IDROutputter.execute_idr_export Error: Failed to export {file_name_stub}")
            print(f"IDROutputter Error: This could be due to invalid SQL syntax, connection issues, or permission problems")
            print(f"IDROutputter Error Details: {str(e)}")
            
            # Show truncated query for debugging
            query_preview = select_query[:200] + "..." if len(select_query) > 200 else select_query
            print(f"IDROutputter Error: SQL Query attempted: {query_preview}")
            raise
    
    @abstractmethod
    def getSelectQuery(self) -> str:
        """
        Abstract method that must be implemented by each subclass.
        
        Returns:
            str: The SELECT query string for this specific export
            
        Note:
            This method must return only the SELECT query portion.
            The COPY INTO wrapper is handled by execute_idr_export().
        """
        pass
        
    def do_idr_output(self, *, file_name_stub: str) -> None:
        """
        Main orchestration method that coordinates the export process.
        
        This method:
        1. Calls getSelectQuery() to get the subclass-specific SELECT query
        2. Calls the static execute_idr_export() method to perform the actual export
        
        Args:
            file_name_stub: Base filename for the export (without timestamp/extension)
            
        Raises:
            Exception: If query generation or export execution fails
        """
        try:
            print(f"IDROutputter: Starting IDR output process for {file_name_stub}")
            print(f"IDROutputter: Using version {self.version_number} (file version: {self.version})")
            
            # Get the SELECT query from the subclass
            select_query = self.getSelectQuery()
            
            if not select_query or not select_query.strip():
                raise ValueError("getSelectQuery() returned empty or null query")
                
            print(f"IDROutputter: Retrieved SELECT query from subclass ({len(select_query)} characters)")
            
            # Execute the export using the static method
            self.execute_idr_export(
                select_query=select_query, 
                file_name_stub=file_name_stub,
                version_number=self.version_number
            )
            
            print(f"IDROutputter: Completed IDR output process for {file_name_stub}")
            
        except Exception as e:
            print(f"IDROutputter.do_idr_output Error: Failed to complete export for {file_name_stub}")
            print(f"IDROutputter Error: This indicates either getSelectQuery() failed or export execution failed")
            print(f"IDROutputter Error Details: {str(e)}")
            print(f"IDROutputter Error: Check your SELECT query syntax and Snowflake connection")
            raise
