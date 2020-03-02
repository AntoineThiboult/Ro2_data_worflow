# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 15:12:45 2019

@author: ANTHI182
"""
import os
import re
import pandas as pd
import numpy as np
import subprocess
import shutil
import fileinput
from datetime import datetime as dt
from glob import glob

# TODO manage error and warning codes to final CSV with log file

def vickers_spikes(data, slide_window=3000, upbound=500, lowbound=-50):
    """Detect spikes in data series according to :
    DEAN VICKERS AND L. MAHRT, Quality Control and Flux Sampling Problems for Tower and Aircraft Data, 
    Journal of atmospheric and oceanic technologies
    Input: pandas dataframe column
    Output: index of spikes"""
    # TODO return index of spikes instead of despiked series
    
    # Initial standard deviation tolerance
    std_threshold = 3.5
    # Initialization of outlier lists
    outliers_idx = [True]
    # Hard copy of input
    data_trimmed = data.copy()
    
    while any(outliers_idx):
        # Sliding windows
        data_rolled_mean = data.rolling(slide_window,center=True, min_periods=1).mean()
        data_rolled_std  = data.rolling(slide_window,center=True, min_periods=1).std()
        outliers_idx = abs(data) > abs(data_rolled_mean + (std_threshold * data_rolled_std) )
        data_trimmed[outliers_idx] = np.nan
        std_threshold += 0.1
    
    # Absolute min and max
    amm_outliers_idx = (data > upbound) | (data < lowbound)
    data_trimmed[amm_outliers_idx] = np.nan
    
    return data_trimmed


def rename_trim_vars(stationName,df):

    print('Start renaming variables for station:', stationName, '...', end='\r')
    # Import Excel documentation file
    xlsFile = pd.ExcelFile('./Resources/EmpreinteVariableDescription.xlsx')
    column_dic = pd.read_excel(xlsFile,stationName)

    # Make translation dictionary from CS vars to DB vars
    lines_to_include = column_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
    column_dic = column_dic[lines_to_include == False]
    column_dic = column_dic.iloc[:,[0,1]]
    column_dic.columns = ['db_name','cs_name']

    # Trim dataframe and rename columns
    idColumnsIntersect = column_dic.cs_name.isin(df.columns)
    df = df[column_dic.cs_name[idColumnsIntersect]]
    df.columns = column_dic.db_name[idColumnsIntersect]
    
    # Merge columns that have similar column name
    if df.keys().shape != df.keys().unique().shape:
        df = df.groupby(df.columns, axis=1).mean()
    print('Done!')

    return df


def load_eddypro_file(stationName,inputDir):

    print('Start loading eddy pro file for station:', stationName, '...', end='\r')
    # List eddy pro output files and select most recent one
    eddyFullOutputList = glob(inputDir+'/'+stationName+'/'+'\*full_output*.csv') # TODO chose between glob and os.listdir
    eddyProFileToLoad = max(eddyFullOutputList, key=os.path.getctime) # Select latest file

    # Import as dataframe
    df = pd.read_csv(eddyProFileToLoad,skiprows=[0,2])

    # Create a standardized time column
    df['TIMESTAMP']=pd.to_datetime(df.date.map(str) +" "+ df.time.map(str), yearfirst=True)
    print('Done!')
    
    return df


def convert_CSbinary_to_csv(stationName,rawFileDir,asciiOutDir):

    # TODO check compatibility with unix and Wine
    # TODO solve issue with shutil.copy that overwrite previous file. Add iDataCollection to name
    
    print('Start converting Campbell binary files to csv for station:', stationName)
    
    # Open error log file
    logf = open("convert_CSbinary_to_csv.log", "w")

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]

    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern Station_YYYYMMDD
        sationNameRegex=r'^' + stationName + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]

        for iDataCollection in listDataCollection:
            print(iDataCollection)
            
            # Check if conversion has already been performed
            destDirContent = os.listdir(os.path.join(asciiOutDir,stationName))
            subStr = re.search(r'^.*([0-9]{8})$', iDataCollection).group(1)
            
            if not [f for f in destDirContent if re.match(subStr, f)]:       
            
                for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection)):
                    print('\t'+rawFile)
    
                    inFile=os.path.join(rawFileDir,iFieldCampain,iDataCollection,rawFile)
                    outFile=os.path.join(asciiOutDir,stationName,rawFile)
    
                    try:
                        # File type name handling
                        if bool(re.search("ts_data_",rawFile)) | bool(re.search("_Time_Series_",rawFile)):
                            extension="_eddy.csv"
                        elif bool(re.search("alerte",rawFile)):
                            extension="_alert.csv"
                        elif bool(re.search("_Flux_CSIFormat_",rawFile)) | bool(re.search("flux",rawFile)) | bool(re.search("data_",rawFile)):
                            extension="_slow.csv"
                        elif bool(re.search("radiation",rawFile)) | bool(re.search("_Flux_Notes_",rawFile)):
                            extension="_slow2.csv"
                        elif bool(re.search("met30min",rawFile)):
                            extension="_slow3.csv"
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
                    except Exception as e:
                        print(str(e))
                        logf.write("Failed to convert {0} from bin to csv: {1} \n".format(inFile, str(e)))

    # Close error log file
    logf.close()
    
    print('Done!')


def batch_process_eddypro(stationName,asciiOutDir,eddyproConfigDir,eddyproOutDir,dates):

    # TODO check compatibility with unix and Wine
    # TODO check if the path must be absolute
    # TODO manage error code with subprocess.call and add exception
    
    print('Start Eddy Pro processing for station:', stationName, '...', end='\r')
    
    eddyproOutDir   = eddyproOutDir + stationName
    eddyproConfig   = eddyproConfigDir + "Ro2_" + stationName + ".eddypro"
    eddyproMetaData = eddyproConfigDir + "Ro2_" + stationName + ".metadata"
    asciiOutDir     = asciiOutDir + stationName

    # Read in the Eddy Pro config file and replace target strings
    with fileinput.FileInput(eddyproConfig, inplace=True) as file:
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
                line = re.sub(r'^pr_start_date=.*$',"pr_start_date="+dates['start'], line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_time',line):
                line = re.sub(r'^pr_start_time=.*$',"pr_start_time="+"00:00", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_date',line):
                line = re.sub(r'^pr_end_date=.*$',"pr_end_date="+dates['end'], line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_time',line):
                line = re.sub(r'^pr_end_time=.*$',"pr_end_time="+"00:00", line.rstrip())
                print(line,end='\n')
            else:
                print(line,end='')

    process=os.path.join(".\Bin","EddyPro","bin","eddypro_rp.exe")
    subprocess.call([process, eddyproConfig])
    
    print('Done!')

def merge_thermistors(dates, rawFileDir,mergedCsvOutDir):
    
    print('Start merging thermistors data')
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]
    
    counterField = 0
    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern TidBit_YYYYMMDD
        sationNameRegex = r'^' + 'TidBit' + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]

        for iDataCollection in listDataCollection:

            #Find all thermistor files in folder
            thermNameRegex = r'^' + 'Therm' + r'[1-2].*xlsx$'
            listThermSensors = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection,'Excel_exported')) if re.match(thermNameRegex, f)]
            
            counter = 0
            for iSensor in listThermSensors:
                
                # Load data
                df_tmp = pd.read_excel(os.path.join(rawFileDir,iFieldCampain,iDataCollection,'Excel_exported',iSensor), skiprows=[0])

                # Making nice variable names
                tmp_sensorNiceName = re.sub('\.','m',iSensor,1)[0:-5]
                sensorNiceNameTemp = 'water_temp_' + re.split('_',tmp_sensorNiceName)[1] + '_' + re.split('_',tmp_sensorNiceName)[0]
                sensorNiceNamePress = 'water_height_' + re.split('_',tmp_sensorNiceName)[1] + '_' + re.split('_',tmp_sensorNiceName)[0]


                # Remove log columns
                listCol = [c for c in df_tmp.columns if re.match('.*(Date|Temp|Pres).*', c)]
                df_tmp = df_tmp[listCol]
                df_tmp.index = pd.to_datetime(df_tmp.iloc[:,0])
                df_tmp = df_tmp.loc[~df_tmp.index.duplicated(keep='first')]

                # Fill df with records
                if df_tmp.shape[1] == 2: # Temperature only sensor
                    idDates_RecInRef = df_tmp.index.isin(df.index)
                    idDates_RefInRec = df.index.isin(df_tmp.index)
                    df.loc[idDates_RefInRec,sensorNiceNameTemp] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[1]]

                elif df_tmp.shape[1] == 3: # Temperature and pressure sensor
                    idDates_RecInRef = df_tmp.index.isin(df.index)
                    idDates_RefInRec = df.index.isin(df_tmp.index)
                    df.loc[idDates_RefInRec,sensorNiceNameTemp] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[2]]
                    df.loc[idDates_RefInRec,sensorNiceNamePress] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[1]]
                
                print("\rMerging thermistors for dataset {:s}, progress {:2.1%} ".format(iFieldCampain, counter/len(listThermSensors)), end='\r')
                counter += 1

    df['timestamp'] = df.index
    df = df.reindex(sorted(df.columns), axis=1)
    df.to_csv(os.path.join(mergedCsvOutDir,'Thermistors.csv'), index=False)
    print('Done!')

def merge_slow_csv(stationName,asciiOutDir,dates):
    
    print('Start merging slow data for station:', stationName, '...', end='\r')
    # Module to merge same type of slow data together
    def merge_slow_data(slow_df, slowList):
       
        for iSlow in slowList:
            tmp_df = pd.read_csv(os.path.join(asciiOutDir,stationName,iSlow), sep=',',skiprows=[0,2,3], low_memory=False)            
            slow_df = slow_df.append(tmp_df)            
        slow_df = slow_df.drop_duplicates(subset='TIMESTAMP', keep='last')

        return slow_df

    # List all slow csv files and merge them together
    fileInDir = os.listdir(os.path.join(asciiOutDir,stationName))
    fileInDir.sort()
    
    # Slow data
    slowList = [s for s in fileInDir if re.match('.*slow\.csv', s)]
    slow_df = pd.DataFrame()
    slow_df = merge_slow_data(slow_df, slowList)

    # Slow data 2
    slowList2 = [s for s in fileInDir if re.match('.*slow2\.csv', s)]
    if slowList2:
        slow_df2 = pd.DataFrame()
        slow_df2 = merge_slow_data(slow_df2, slowList2)        
        nonDuplicatedColumns = slow_df2.columns[~slow_df2.columns.isin(slow_df.columns)].append(pd.Index(['TIMESTAMP']))
        slow_df = slow_df.merge(slow_df2[nonDuplicatedColumns], how='left', on='TIMESTAMP')

    # Create the TIMESTAMP column that will be used for merging with other df
    slow_df['TIMESTAMP'] = pd.to_datetime(slow_df['TIMESTAMP'])
    
    # Converts everything but 'TIMESTAMP' to float
    slow_df.loc[:,slow_df.columns != 'TIMESTAMP'] = slow_df.loc[:,slow_df.columns != 'TIMESTAMP'].astype(float)
    
    print('Done!')
    
    return slow_df


def merge_slow_csv_and_eddypro(stationName,slow_df,eddy_df, mergedCsvOutDir):
    
    print('Start merging slow and Eddy Pro data for station:', stationName, '...', end='\r')
    
    # Merge and save
    merged_df = eddy_df.merge(slow_df, on='timestamp', how='left')
    
    print('Done!')
    
    return merged_df

def gap_fill(stationName,df,mergedCsvOutDir,gapfillConfig):

    # load gap filling config file
    xlsFile = pd.ExcelFile('./Config/GapFillingConfig/gapfilling_configuration.xlsx')
    df_config = pd.read_excel(xlsFile,stationName+'_MDS')
    
    # Check that all proxy vars are available for gap filling
    if 'Alternative_station' in df_config.columns:
        nAlternative_station = sum(~df_config['Alternative_station'].isna())
        for iAlternative_station in range(nAlternative_station):
            # Load alternative station data
            df_altStation = pd.read_csv("C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/"+
                                        df_config.loc[iAlternative_station,'Alternative_station']+'.csv')
            df_altStation = df_altStation[ ['timestamp',df_config.loc[iAlternative_station,'Proxy_vars_alternative_station']] ]
            df = df.merge(df_altStation, on='timestamp', how='left')            
            
    # Handle special case of berge and reservoir
    if stationName in ['Berge', 'Reservoir']:                
        df['water_temp_surface'] = df[['water_temp_0m0_Therm1','water_temp_0m0_Therm2',
                                       'water_temp_0m4_Therm1','water_temp_0m4_Therm2']].mean(axis=1)        
        df['delta_Tair_Teau'] = abs(df['water_temp_surface'] - df['air_temp_IRGASON107probe'])
        
    for iVar_to_fill in df_config.loc[~df_config['Vars_to_fill'].isna(),'Vars_to_fill']:        
        print('\nStart gap filling for variable {:s} and station {:s}'.format(iVar_to_fill, stationName))
        df = gapfill_mds(df,iVar_to_fill,df_config,mergedCsvOutDir)

    return df
    
def gapfill_mds(df,var_to_fill,df_config,mergedCsvOutDir):
    # Coded from Reichtein et al. 2005
    #
    # Flowchart:
    #
    #   NEE present ?                                                 --> Yes     --> Does nothing
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 7 days                --> Yes     --> Filling quality A (case 1)
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 14 days               --> Yes     --> Filling quality A (case 2)
    #    |
    #    V
    #   Rg, NEE available within |dt|<= 7 days                        --> Yes     --> Filling quality A (case 3)
    #    |
    #    V
    #   NEE available within |dt|<= 1h                                --> Yes     --> Filling quality A (case 4)
    #    |
    #    V
    #   NEE available within |dt|= 1 day & same hour of day           --> Yes     --> Filling quality B (case 5)
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 21, 28,..., 140 days  --> Yes     --> Filling quality B if |dt|<=28, else C (case 6)
    #    |
    #    V
    #   Rg, NEE available within |dt|<= 14, 21, 28,..., 140 days      --> Yes     --> Filling quality B if |dt|<=28, else C (case 7)
    #    |
    #    V
    #   NEE available within |dt|<= 7, 21, 28,...days                 --> Yes     --> Filling quality C (case 8)

    # Submodule to find similar meteorological condition within a given window search
    def find_meteo_proxy_index(df, t, search_window, proxy_vars, proxy_vars_range):        
        proxy_vars.dropna(inplace=True)
        current_met = df.loc[t,proxy_vars]
        if any(current_met.isna()):
            index_proxy_met = None
            fail_code = "NaN in current proxy meteorological vars"
        else:            
            t_loc = df.index.get_loc(t)
            t_start = np.max([0, t_loc-int(search_window*48)])
            t_end = np.min([df.shape[0]-1, t_loc+int(search_window*48)+1])
            time_window = list( range(t_start ,t_end) )
            time_window_met = df.loc[df.index[time_window],proxy_vars]
            index_proxy_met_bool = pd.DataFrame()
            # Check if proxy met matches gap filling conditions
            for iVar in range(0,proxy_vars.shape[0]):
                index_proxy_met_bool = pd.concat(
                    [index_proxy_met_bool, 
                     abs(time_window_met.iloc[:,iVar] - current_met.iloc[iVar]) < proxy_vars_range[iVar]], axis=1)
                
            # Check that the corresponding var_to_fill is not NaN
            index_proxy_met_bool = pd.concat([index_proxy_met_bool, ~df.loc[df.index[time_window],var_to_fill].isna()], axis=1)
            index_proxy_met_bool = index_proxy_met_bool.all(axis=1)
            # Convert bool to index
            index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool == True]
            if index_proxy_met.size == 0:
                fail_code = "Proxy met not found"
            else:
                fail_code = None
        return index_proxy_met, fail_code

    # Submodule to find a NEE within one day at the same hour of day
    def find_nee_proxy_index(df, t, search_window, exact_time=False):
        t_loc = df.index.get_loc(t)
        t_start = np.max([0, t_loc-int(search_window*48)])
        t_end = np.min([df.shape[0]-1, t_loc+int(search_window*48)+1])
        if exact_time:
            time_window = list([t_start ,t_end])
        else:
            time_window = list( range(t_start ,t_end) )
        time_window_met = df.loc[df.index[time_window],var_to_fill]
        index_proxy_met_bool = ~time_window_met.isna()
        index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool == True]
        if index_proxy_met.size == 0:
            fail_code = "Proxy met not found"
        else:
            fail_code = None
        return index_proxy_met, fail_code
        
    # Identify missing flux and time step that should be discarded           
    df[var_to_fill] = vickers_spikes(df[var_to_fill])
    id_rain = df['precip_TB4'] > 0
    id_missing_nee = df.isna()[var_to_fill]
    id_low_quality = df[var_to_fill+'_qf'] == 2
    id_missing_flux = id_rain | id_missing_nee | id_low_quality    
    df.loc[id_missing_flux,var_to_fill] = np.nan
    
    # Add new columns to data frame that contains var_to_fill gapfilled
    gap_fil_col_name = var_to_fill + "_gf_mds"
    df[gap_fil_col_name] = df[var_to_fill]
    gap_fil_quality_col_name = gap_fil_col_name + "_qf"
    df[gap_fil_quality_col_name] = None
    
    # Define variables used for Vars_to_fill
    proxy_vars = var_to_fill + '_proxy_vars'
    proxy_vars_range = var_to_fill + '_proxy_vars_range'
    proxy_vars_subset = var_to_fill + '_proxy_vars_subset'
    
    # Loop over time steps
    for t in df.index[id_missing_flux]:
        if not t%100:
            print("\rGap filling {:s}, progress {:2.1%} ".format(var_to_fill, t/len(df.index)), end='\r')
        
        # Case 1
        search_window = 7
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df.loc[index_proxy_met,var_to_fill])
            df.loc[t,gap_fil_quality_col_name] = "A1"        
            continue

        # Case 2
        search_window = 14
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A2"
            continue

        # Case 3
        search_window = 7
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars_subset], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A3"
            continue

        # Case 4
        search_window = 1/24
        index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window)
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A4"
            continue

        # Case 5
        search_window = 1
        index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window, True)
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "B1"
            continue

        # Case 6
        search_window = 14
        while bool(fail_code) & (search_window <= 140):
            search_window += 7
            index_proxy_met, fail_code = find_meteo_proxy_index(
                df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                if search_window <= 28:
                    df.loc[t,gap_fil_quality_col_name] = "B2"
                else:
                    df.loc[t,gap_fil_quality_col_name] = "C1"
                continue

        # Case 7
        search_window = 7
        while bool(fail_code) & (search_window <= 140):
            search_window += 7
            index_proxy_met, fail_code = find_meteo_proxy_index(
                df, t, search_window, df_config[proxy_vars_subset], df_config[proxy_vars_range])
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                if search_window <= 14:
                    df.loc[t,gap_fil_quality_col_name] = "B3"
                else:
                    df.loc[t,gap_fil_quality_col_name] = "C2"
                continue

        # Case 8
        search_window = 0
        while bool(fail_code) & (search_window <= 140):
            search_window += 7
            index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "C3"
                continue
            
    return df