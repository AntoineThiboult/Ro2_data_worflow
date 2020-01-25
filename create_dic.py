# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 14:48:25 2020

@author: ANTHI182
"""
import pandas as pd
# Import Excel documentation file
xlsFile = pd.ExcelFile('./Resources/EmpreinteVariableDescription.xlsx')
db_dic = pd.read_excel(xlsFile,'Berge')

# Trim data
lines_to_include = db_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
db_dic = db_dic[lines_to_include == False]
db_dic = db_dic.iloc[:,[0,1]]
db_dic.columns = ['db_name','cs_name']




csvFile = pd.read_csv(r'C:\Users\anthi182\Desktop\Micromet_data\Merged_csv\Berge_gapfilled_data.csv')
csvFile = csvFile[db_dic.cs_name]
csvFile.columns = db_dic.db_name