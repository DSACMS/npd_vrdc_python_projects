#!/bin/sh
# Delete the previous run
rm unmerged_csv_files/*.csv
# move to the download directory to being the download
cd ./unmerged_csv_files/
# download using snowsql. You must have cms_idr configured for snowflake
snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
# go back the main directory
cd ..
# merge the new csv file here. 
python3 ../misc_scripts/snowflake_csv_merge.py ./unmerged_csv_files/ --output-dir .
