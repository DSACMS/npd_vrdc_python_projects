# VRDC Python Projects

This is a meta project folder that will have VRDC projects to create new data files inside the VRDC environment. 

## Purpose and Approach

Some of these data files will become public resources on (probably) data.cms.gov. Others will have private information that will require that the data remain in the CMS VRDC environment. 

The projects should generally be implemented using [plainerflow](https://github.com/DSACMS/ndh_plainerflow) so that they will be portable between SQL based systems.

It is possible that some of these systems will be need to be executed in SAS ProcSQL or FedSQL, or snowflake notebooks but the default environment will be databricks notebooks. 
