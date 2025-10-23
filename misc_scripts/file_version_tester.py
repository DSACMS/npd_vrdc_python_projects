"""

This script should accept a --python_dir and --csv_dir argument

Take a look at idr/IDROutputter.py and for an implementing class idr/medicaid_provider_credentials_new.py

Using sqlglot, you can loop over all of the classes that implement the IDROutputter abstract base class in the python_dir and run the getSelectQuery 
function on those classes to get back the SELECT statement and then extract what columns should be in the csv output file from that program using sqlglot. 

You can also grab the file version from the version_number class property. As well as the file_name_stub from the do_idr_output call.

Then loop over the csv files in the csv_dir and grab the first line which contains the headers for each one. 
Also note when there are two files that match a single file name, version number but have different timestamps.
Use wc -l to get the number of lines in each csv file.
The output should be a STDOUT report that shows for each class implementing IDROutputter:

For the version comparision: the version in the file names is between two dots. So a simple string comparision with that value works.

 
Your file name parsing  method should look for the following pattern {file_name_stub}FILLER_WITH_ANTYHING_AT_ALL_INCLUDING_NOTHING{version_number}.{timestamp}.csv
For the multiple file instances.. you should consider THIS_IS_THE_STUB_thisisrandomfiller.v01.2024_10_22_2207.csv and THIS_IS_THE_STUB_anotherdifferentfiller.v01.2024_10_23_1105.csv as colliding files.
If NPPESMainExporter is not honoring the convention.. please change it.  modify the file output in NPPESMainExporter so that the {version_number}.{timestamp}.csv pattern is honored.


The version comparison should not really be "outdated" in the sense that it correctly handles a version number higher. It should just be "matching" or "not matching" for now.

- The file_name_name
- Green "current" if this is the latest version number for this file, there is only one file like this in the directory and the headers match the SELECT statement columns.
- Yellow for any of the rest of these warning status:
    - "outdated version" if there is a newer version number for this file_name_stub
    - "multiple files" if there is more than one file with this file_name_stub and version number but different timestamps
    - "header mismatch" if the headers in the csv file do not match the columns in the SELECT statement

Do not delete these instructions as you implement the code.
"""