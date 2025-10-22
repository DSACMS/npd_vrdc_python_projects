"""
This class leverages the EntityLooper to create the entity map from a time period in VRDC claims files. 

First, it creates a VIEW that contains a row in the data with five columns for each month of claim + claim_line data. 
These are: 

* source_setting_name
* TAX_NUM
* CCN
* onpi
* pnpi
* bene_id
* clm_id

In the end, the entity map is by querying this view with the simple query: 

```sql
CREATE TABLE {output_DBTable}
SELECT 
    COUNT(DISTINCT source_setting_name) AS setting_count,
    TAX_NUM,
    CCN,
    onpi,
    pnpi,
    COUNT(DISTINCT(bene_id)) AS bene_count,
    COUNT(DISTINCT(clm_id)) AS claim_count
FROM {vrdc_entity_map_view}
GROUP BY 
    TAX_NUM,
    CCN,
    onpi,
    pnpi
HAVING COUNT(DISTINCT(bene_id)) > 10
```

inside an f-string so that the variables can be resolved and modified without changing the code. 

The view itself should be built based on the querying each claim and claim-line pair like so: 

```sql
SELECT 
    {get_my_select_variables_for_entity_map_class}
FROM {this_claim_table} AS CLAIM
LEFT JOIN {this_claim_line_table} AS CLAIM_LINE
    ON CLAIM.CLM_ID = CLAIM_LINE.CLM_ID
WHERE {my entity columns are not NULL}
```
you will need to do a DROP VIEW IF EXISTS before creating the view.

The program should accept a start year/month and end year/month to define the time period.
As well as a list of settings to include in the entity map. If no settings are provided, all 7 settings should be included.
Then the view should be built for that time period, as a UNION of all of the monthly data within the time period

Please read the EntityLooper class for more information on how the entity map is built.

Read vrdc/EIN_CCN_NPI_gather.py for an idea of how to access the local spark connection to get this done. 

Make a persistent view here, so that we can test against it in subsequent steps. But lets go ahead and make a little log table at the end that records the number
of rows in the view, the number of distinct TAX_NUM, CCN, onpi, pnpi, bene_id and clm_id and the date/time the view was created.

The start of the file should have the variables that will determine how the file is configured and run. This view will be run in an databricks notebook context.


Do not overwrite these comments as you modify the code.

"""