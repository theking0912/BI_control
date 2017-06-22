#!/usr/bin/env python
# -*- coding:utf-8 -*-
from import_export.data_init import export_csv_init
from import_export.data_init import Import_Csv_To_Oracle_init


export_csv_init.export()
Import_Csv_To_Oracle_init.import_to_oracle()