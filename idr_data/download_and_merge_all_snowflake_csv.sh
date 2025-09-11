#!/bin/sh
cd ./unmerged_csv_files/
#snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
cd ..
python3 ../misc_scripts/snowflake_csv_merge.py ./unmerged_csv_files/ --output-dir .
