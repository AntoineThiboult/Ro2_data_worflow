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
from datetime import datetime as dt
import fileinput


def convert_CSbinary_to_csv(rawFileDir,asciiOutDir):

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]
    
    for iFieldCampain in listFieldCampains:
        
        #Find folders that match the pattern Station_YYYYMMDD
        listStations  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(r'^(Berge|Reservoir|Foret_ouest|Foret_est|Foret_sol)_[0-9]{8}$', f)]
        
        for iStation in listStations:
            print(iStation)
            stationName=iStation[0:-9]
            for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iStation)):              
                print(rawFile)  
                
                inFile=os.path.join(rawFileDir,iFieldCampain,iStation,rawFile)
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
                    shutil.copy(inFile,outFile)
                    continue
                
                # Conversion from the Campbell binary file to csv format
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

def batch_process_eddypro(rawFileDir,asciiOutDir,eddyproOutDir):
    
    process=os.path.join(".\Bin","EddyPro","eddypro_rp.exe")
    eddyproConfig=os.path.join(".\EddyProConfig","RO2_Berge.eddypro")
    eddyproMetaData=os.path.join(".\EddyProConfig","RO2_Berge.metadata")
        
    # Read in the Eddy Pro config file and replace target strings
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
            else:
                print(line,end='')
      
    subprocess.call([process, eddyproConfig])
    # TODO modify the eddypro config file to match the new file input format
