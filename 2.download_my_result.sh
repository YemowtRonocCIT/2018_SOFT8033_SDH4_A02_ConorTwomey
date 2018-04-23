#!/usr/bin/env bash

MYPATH="./my_result/"
dbfs cp -r "dbfs:/FileStore/tables/A02/my_result" "$MYPATH"
python "3. merge_solutions.py" "solution.txt" "$MYPATH/"
