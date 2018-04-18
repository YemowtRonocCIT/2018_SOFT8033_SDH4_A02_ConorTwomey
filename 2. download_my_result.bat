set mypath=%cd%\my_result
databricks fs cp -r "dbfs:/FileStore/tables/A02/my_result" "%mypath%"
python.exe "3. merge_solutions.py" "solution.txt" "%mypath%\\"



