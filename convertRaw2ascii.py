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


station="Berge"
rawFileDir      =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Raw_data")
asciiFileDir=os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Ascii_data")


#def printnames(station,rawFileDir,asciiFileDir):

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
            outFile=os.path.join(asciiFileDir,stationName,rawFile)
            
            # File type name handling
            if bool(re.search(".ts_data_",rawFile)):
                extension="_eddy.csv" 
            elif bool(re.search(".alerte",rawFile)):
                extension="_alert.csv"         
            elif bool(re.search(".radiation",rawFile)):
                extension="_radiation.csv" 
            elif bool(re.search(".met30min",rawFile)):
                extension="_slow.csv" 
            else:                           # .cr1 / .cr3 / sys_log files
                shutil.copy(inFile,outFile)
                continue
            
            # Conversion from the Campbell binary file to csv format
            # TODO Check if the the file already exists 
            process=os.path.join(".\Bin","raw2ascii","csidft_convert.exe")
            subprocess.call([process, inFile, outFile, 'ToA5'])
            
            # Rename file according to date
            fileContent=pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3], nrows=1)
            try:
                fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S")    # TIMESTAMP format for _alert.csv, _radiation.csv, and _met30min.csv
            except:
                fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S.%f") # TIMESTAMP format for _eddy.csv file
            newFileName=dt.strftime(fileStartTime,'%Y%m%d')+extension
            shutil.move(outFile,os.path.join(asciiFileDir,stationName,newFileName))

    
