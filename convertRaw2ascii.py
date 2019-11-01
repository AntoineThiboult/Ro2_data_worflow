# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 15:12:45 2019

@author: ANTHI182
"""
import os
import re
import pandas as pd
import subprocess
import fnmatch

def printnames(station,rawFileDir,asciiFileDir):

    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]  #Find folders that match Ro2_YYYYMMDD
    for iFieldCampain in listFieldCampains:
        listStations  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(r'^(Berge|Reservoir|Foret_ouest|Foret_est|Foret_sol)_[0-9]{8}$', f)]  #Find folders that match Ro2_YYYYMMDD
        for iStation in listStations:
            print(iStation)
            stationName=iStation[0:-9]
            for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iStation)):
                if fnmatch.fnmatch(rawFile, "*.ts_data_*"):            
                    print(rawFile)
                    process=os.path.join(".\Bin","raw2ascii","csidft_convert.exe")
                    inFile=os.path.join(rawFileDir,iFieldCampain,iStation,rawFile)
                    outFile=os.path.join(asciiFileDir,stationName,rawFile)
#                    subprocess.call([process, inFile, outFile, 'ToA5'])
                    a=pd.read_csv(outFile, sep=',', index_col=None, skiprows=5)
                    break
