# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 15:12:45 2019

@author: ANTHI182
"""
import os
import re
import pandas as pd
import subprocess
import shutil
import fileinput
from datetime import datetime as dt #TODO , timedelta as td

def convert_CSbinary_to_csv(stationName,rawFileDir,asciiOutDir):

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]
    
    for iFieldCampain in listFieldCampains:
        
        #Find folders that match the pattern Station_YYYYMMDD
        sationNameRegex=r'^' + stationName + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]
        
        for iDataCollection in listDataCollection:
            print(iDataCollection)
            for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection)):              
                print('\t'+rawFile)  
                
                inFile=os.path.join(rawFileDir,iFieldCampain,iDataCollection,rawFile)
                outFile=os.path.join(asciiOutDir,stationName,rawFile)
                
                # File type name handling           
                if bool(re.search("ts_data_",rawFile)) | bool(re.search("_Time_Series_",rawFile)):
                    extension="_eddy.csv" 
                elif bool(re.search("alerte",rawFile)):
                    extension="_alert.csv"         
                elif bool(re.search("met30min",rawFile)) | bool(re.search("_Flux_CSIFormat_",rawFile)) | bool(re.search("flux",rawFile)):
                    extension="_slow.csv" 
                elif bool(re.search("radiation",rawFile)) | bool(re.search("_Flux_Notes_",rawFile)):
                    extension="_slow2.csv"             
                else:                           # .cr1 / .cr3 / sys_log files / Config_Setting_Notes / Flux_AmeriFluxFormat_12
                    shutil.copy(inFile,outFile) # TODO solve issue: file with same name will overwrite
                    continue
                
                # Conversion from the Campbell binary file to csv format
                # TODO check compatibility with unix and Wine
                process=os.path.join(".\Bin","raw2ascii","csidft_convert.exe")
                subprocess.call([process, inFile, outFile, 'ToA5'])
                
                # Rename file according to date
                fileContent=pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3], nrows=1)
                try:
                    fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S")    # TIMESTAMP format for _alert.csv, _radiation.csv, and _met30min.csv
                except:
                    fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S.%f") # TIMESTAMP format for _eddy.csv file
                newFileName=dt.strftime(fileStartTime,'%Y%m%d_%H%M')+extension
                shutil.move(outFile,os.path.join(asciiOutDir,stationName,newFileName))

def batch_process_eddypro(iStation,asciiOutDir,eddyproConfig,eddyproMetaData,eddyproOutDir):
              
    eddyproOutDir   = eddyproOutDir + iStation
    eddyproConfig   = eddyproConfig + "Ro2_" + iStation + ".eddypro"
    eddyproMetaData = eddyproMetaData +"Ro2_"+ iStation + ".metadata"
    asciiOutDir     = asciiOutDir + iStation
    
    # Read in the Eddy Pro config file and replace target strings
    # TODO check if the path must be absolute
    with fileinput.FileInput(eddyproConfig, inplace=True, backup='.bak') as file:
        for line in file:
            if re.match(r'file_name',line):
                line = re.sub(r'^file_name=.*$',"file_name="+eddyproConfig, line.rstrip())
                print(line,end='\n')
            elif re.match(r'proj_file',line):
                line = re.sub(r'^proj_file=.*$',"proj_file="+eddyproMetaData, line.rstrip())
                print(line,end='\n')
            elif re.match(r'out_path',line):
                line = re.sub(r'^out_path=.*$',"out_path="+eddyproOutDir, line.rstrip())
                print(line,end='\n')
            elif re.match(r'data_path',line):
                line = re.sub(r'^data_path=.*$',"data_path="+asciiOutDir, line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_date',line):
                line = re.sub(r'^pr_start_date=.*$',"pr_start_date="+"2019-03-27", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_time',line):
                line = re.sub(r'^pr_start_time=.*$',"pr_start_time="+"13:00", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_date',line):
                line = re.sub(r'^pr_end_date=.*$',"pr_end_date="+"2019-03-30", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_time',line):
                line = re.sub(r'^pr_end_time=.*$',"pr_end_time="+"00:00", line.rstrip())
                print(line,end='\n')
            else:
                print(line,end='')
            # TODO add line to modify dates + round up to next 30 minutes : dt.now() + (dt.min - dt.now()) % td(minutes=30)            
            # TODO pr_subset=0 for "select a different period"    
            
    # TODO check compatibility with unix and Wine
    process=os.path.join(".\Bin","EddyPro","eddypro_rp.exe")    
    subprocess.call([process, eddyproConfig])    
