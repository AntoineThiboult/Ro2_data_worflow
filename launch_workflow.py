import pandas as pd
#from joblib import Parallel, delayed
import process_ro2_data as prd



allStations     = ["Berge","Reservoir","Foret_ouest","Foret_est","Foret_sol"]
eddyCovStations = ["Berge","Reservoir","Foret_ouest","Foret_est"]

rawFileDir      = "E:/Ro2_micromet_raw_data/Data"
asciiOutDir     = "C:/Users/anthi182/Desktop/Micromet_data/Ascii_data/"
eddyproOutDir   = "C:/Users/anthi182/Desktop/Micromet_data/Eddypro_data/"
eddyproConfig   = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/EddyProConfig/"
mergedCsvOutDir = "C:/Users/anthi182/Desktop/Micromet_data/Merged_csv"

for iStation in allStations:

    # Binary to ascii
    prd.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)

    if iStation in eddyCovStations:
        # Ascii to eddypro
        prd.batch_process_eddypro(iStation,asciiOutDir,eddyproConfig,eddyproOutDir)
        # Merge eddy data and slow data
        prd.merge_eddy_and_slow(iStation,asciiOutDir,eddyproOutDir,mergedCsvOutDir)

    # Rename and trim variables

        # Perform gap filling
        var_to_fill = "LE"
        if (iStation == "Berge") | (iStation == "Foret_ouest") | (iStation == "Foret_est"):
            met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50, "HMP45C_Sensor_temp_Avg" : 2.5,"vpd" : 500}, index=[0] )
        elif iStation == "Reservoir":
            met_vars = pd.DataFrame({"SWUpper_Avg" : 50, "air_temperature" : 2.5}, index=[0] )
            prd.flux_gap_filling(iStation,var_to_fill,met_vars,mergedCsvOutDir)

# Merge Hobo TidBit thermistors
prd.merge_thermistors(rawFileDir, mergedCsvOutDir)