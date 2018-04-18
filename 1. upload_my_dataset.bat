set mypath=%cd%\my_dataset
databricks fs rm -r "dbfs:/FileStore/tables/A02/my_dataset" 
databricks fs cp -r "%mypath%" "dbfs:/FileStore/tables/A02/my_dataset" 
