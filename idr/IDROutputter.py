"""
Please read every python file in this directory and notice how repetative the currently are. 
It is the same script, repeating again and again, with slightly different SQL queries and different file output formats. 

I would like to make a class that handles this in the following manner: 

* Have a static class that accepts a sql SELECT query, and an output file name and performs the output. This class should put SELECT query into the COPY INTO {some_file_name}
FROM ( {select_query} ) string.. execute the query in the same manner as performed in each of these files. 
* There should be a must-override method on the class called getSelectQuery() that each subclass must implement to return the SELECT query string.
* Each object should have the version identifier of the file version as an class property
* The "tail" of each import file should instantiate the class and call the do_idr_output() method to perform the output. That method should call the getSelectQuery() method to get the query strinng and then pass that the query to the static class method to perform the output.

Please use try catch blocks to handle errors in the SQL execution and file output. These will run in a notebook environment to start so please try to convert any errors into human readable error messages
Given that it is a notebook envronment, for now please forgo logging and just print the error messages.
You can assume as the examples do that there is already an active Snowflake session.
The file name variable should be something like file_name_stub = 'my_good_file_name_stub' 
and the program should leverage this to a file name in a file name creattion f-string like {file_name_stub}.{version_number}{timestamp}.csv
obivously version_number should also be a class property. If a class does not define a version number it should default to v01.

Do not override these instructions as you code. 

TODO: 
* please add the documentation for the class and its methods beneath this TODO section. 
* remove the multiple version number properties. 
* I replaced the seperator between the version number and the timestamp with an underscore '.' DO NOT UNDO THIS CHANGE.

IDROutputter Class Documentation

OVERVIEW:
The IDROutputter class provides a standardized framework for exporting data from Snowflake to CSV files.
It eliminates code duplication across IDR export scripts by providing a common interface and implementation.

ARCHITECTURE:
- Abstract base class that must be subclassed
- Static method handles the actual export execution
- Subclasses provide their specific SELECT queries via getSelectQuery()
- Automatic file naming with version numbers and timestamps

USAGE PATTERN:
1. Create a subclass of IDROutputter
2. Set the version_number class property (defaults to "v01")  
3. Implement getSelectQuery() to return your SELECT statement
4. Instantiate your class and call do_idr_output(file_name_stub="your_filename")

FILE NAMING:
Generated filenames follow the pattern: {file_name_stub}.{version_number}.{timestamp}.csv
Example: medicaid_provider_credentials.v01.2024_10_22_2207.csv

ERROR HANDLING:
All methods include comprehensive error handling with human-readable messages
suitable for notebook environments. Errors are printed rather than logged.


"""

from abc import ABC, abstractmethod
from datetime import datetime
from snowflake.snowpark.context import get_active_session  # type: ignore


class IDROutputter(ABC):
    """
    Abstract base class for standardizing IDR export operations across repetitive export scripts.
    
    This class provides a common framework for Snowflake COPY INTO operations, eliminating
    the code duplication found across IDR export files. Each subclass needs only to implement
    getSelectQuery() with their specific SELECT statement.
    
    Attributes:
        version_number (str): Version identifier used in output filename. Defaults to "v01".
    
    Example:
        class MyExporter(IDROutputter):
            version_number = "v02"
            
            def getSelectQuery(self):
                return "SELECT * FROM my_table"
        
        exporter = MyExporter()
        exporter.do_idr_output(file_name_stub="my_export")
    """
    
    # Class property with default
    version_number: str = "v01"
    
    @staticmethod
    def _execute_export(*, select_query: str, file_name_stub: str, version_number: str) -> None:
        """
        Static method that accepts a SELECT query and file name stub, performs the output.
        Puts SELECT query into COPY INTO {some_file_name} FROM ( {select_query} ) string
        and executes the query in the same manner as performed in the existing files.
        """
        try:
            session = get_active_session()
            ts = datetime.now().strftime("%Y_%m_%d_%H%M")
            file_name = f"@~/{file_name_stub}.{version_number}.{ts}.csv"
            
            copy_into_sql = f"""
COPY INTO {file_name}
FROM (
{select_query}
)""" + """
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = NONE
)
HEADER = TRUE
OVERWRITE = TRUE;
"""
            
            print(f"Starting export for {file_name_stub} to {file_name}")
            session.sql(copy_into_sql).collect()
            print(f"Successfully completed export for {file_name_stub}")
            
        except Exception as e:
            print(f"IDROutputter._execute_export Error: Failed to export {file_name_stub}")
            print(f"Error details: {str(e)}")
            print(f"This could be due to invalid SQL syntax, connection issues, or permission problems")
            raise
    
    @abstractmethod
    def getSelectQuery(self) -> str:
        """
        Must-override method that each subclass must implement to return the SELECT query string.
        """
        pass
        
    def do_idr_output(self, *, file_name_stub: str) -> None:
        """
        Main method that calls getSelectQuery() to get the query string 
        and then passes that query to the static class method to perform the output.
        """
        try:
            print(f"do_idr_output: Starting process for {file_name_stub} (version {self.version_number})")
            
            # Call getSelectQuery() method to get the query string
            select_query = self.getSelectQuery()
            
            if not select_query or not select_query.strip():
                raise ValueError("getSelectQuery() returned empty or null query")
            
            # Pass the query to the static class method to perform the output
            self._execute_export(
                select_query=select_query, 
                file_name_stub=file_name_stub,
                version_number=self.version_number
            )
            
            print(f"do_idr_output: Completed process for {file_name_stub}")
            
        except Exception as e:
            print(f"IDROutputter.do_idr_output Error: Failed to complete output for {file_name_stub}")
            print(f"Error details: {str(e)}")
            print(f"Check your getSelectQuery() implementation and Snowflake connection")
            raise
