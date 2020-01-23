import os
import pandas as pd
#from joblib import Parallel, delayed
import process_ro2_data as prd


#% Binary to ascii
#listStations    = ["Berge","Reservoir","Foret_ouest","Foret_est","Foret_sol"]
#rawFileDir      = os.path.join("E:\\","Ro2_micromet_raw_data","Data")
#asciiOutDir     = os.path.join("C:\\","Users","anthi182","Desktop","Micromet_data","Ascii_data")
#Parallel(n_jobs=5)(delayed(prd.convert_CSbinary_to_csv)(iStation,rawFileDir,asciiOutDir) for iStation in listStations)
#for iStation in listStations:
#    prd.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)


#% Ascii to eddypro
#listStations    = ["Berge","Reservoir","Foret_ouest","Foret_est"]
#asciiOutDir     = "C:/Users/anthi182/Desktop/Micromet_data/Ascii_data/"
#eddyproOutDir   = "C:/Users/anthi182/Desktop/Micromet_data/Eddypro_data/"
#eddyproConfig   = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/EddyProConfig/"
##Parallel(n_jobs=5)(delayed(prd.batch_process_eddypro)(iStation,asciiOutDir,eddyproConfig,eddyproMetaData,eddyproOutDir) for iStation in listStations)
#for iStation in listStations:
#    prd.batch_process_eddypro(iStation,asciiOutDir,eddyproConfig,eddyproOutDir)

#% Merge Hobo TidBit thermistors
rawFileDir      = os.path.join("E:\\","Ro2_micromet_raw_data","Data")
mergedCsvOutDir = os.path.join("C:\\","Users","anthi182","Desktop","Micromet_data","Merged_csv")
prd.merge_thermistors(rawFileDir, mergedCsvOutDir)

#% Merge eddy data and slow data
#asciiOutDir     = os.path.join("C:\\","Users","anthi182","Desktop","Micromet_data","Ascii_data")
#eddyproOutDir   = os.path.join("C:\\","Users","anthi182","Desktop","Micromet_data","Eddypro_data")
#mergedCsvOutDir = os.path.join("C:\\","Users","anthi182","Desktop","Micromet_data","Merged_csv")
#for iStation in listStations:
##iStation="Berge"
#    prd.merge_eddy_and_slow(iStation,asciiOutDir,eddyproOutDir,mergedCsvOutDir)

#% Perform gap filling
#var_to_fill = "LE"
#for iStation in listStations:
#    if (iStation == "Berge") | (iStation == "Foret_ouest") | (iStation == "Foret_est"):
#        met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50, "HMP45C_Sensor_temp_Avg" : 2.5,"vpd" : 500}, index=[0] )
#    elif iStation == "Reservoir":
#        met_vars = pd.DataFrame({"SWUpper_Avg" : 50, "air_temperature" : 2.5}, index=[0] )
#    prd.flux_gap_filling(iStation,var_to_fill,met_vars,mergedCsvOutDir)