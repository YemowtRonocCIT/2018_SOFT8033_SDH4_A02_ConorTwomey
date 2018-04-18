#!/usr/bin/env bash

MYPATH="./my_dataset"
dbfs rm -r "dbfs:/FileStore/tables/A02/my_dataset"
dbfs cp -r "$MYPATH" "dbfs:/FileStore/tables/A02/my_dataset"

