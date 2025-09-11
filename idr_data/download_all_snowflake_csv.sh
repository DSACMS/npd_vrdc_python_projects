#!/bin/sh
snowsql -c cms_idr -q "GET @~/ file://. PATTERN='.*.csv';"
