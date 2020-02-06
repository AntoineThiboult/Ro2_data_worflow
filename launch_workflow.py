import pandas as pd
#from joblib import Parallel, delayed
import process_ro2_data as prd

# TODO deal with start/end dates, especially in merge_therm and eddypro
# TODO add  condition on convert_CSbinary_to_csv to avoid re-conversion
# TODO think of thermistor vars renaming
# TODO add a row for units

### Define paths

allStations     = ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
eddyCovStations = ["Berge","Foret_ouest","Foret_est","Reservoir"]

rawFileDir          = "E:/Ro2_micromet_raw_data/Data"
asciiOutDir         = "E:/Ro2_micormet_processed_data/Ascii_data/"
eddyproOutDir       = "C:/Users/anthi182/Desktop/Micromet_data/Eddypro_data/"
eddyproConfigDir    = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/Config/EddyProConfig/"
mergedCsvOutDir     = "C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/"
gapfillConfigDir    = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/Config/GapFillingConfig/"

dates = {'start':'2019-04-10','end':'2019-04-11'}

### Process stations

# # Merge Hobo TidBit thermistors
# prd.merge_thermistors(rawFileDir, mergedCsvOutDir)

allStations     = ["Berge"]
for iStation in allStations:

    # # Binary to ascii
    # prd.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)

    if iStation in eddyCovStations:

        # Ascii to eddypro
        prd.batch_process_eddypro(iStation,asciiOutDir,eddyproConfigDir,eddyproOutDir,dates)
        
        # # Load eddypro file
        # eddy_df = prd.load_eddypro_file(iStation,eddyproOutDir)
        
    #     # Rename and trim eddy variables
    #     eddy_df = prd.rename_trim_vars(iStation,eddy_df)

    # # Merge slow data
    # slow_df = prd.merge_slow_csv(iStation,asciiOutDir)
    # # Rename and trim slow variables
    # slow_df = prd.rename_trim_vars(iStation,slow_df)

    # # Merge slow and eddy data
    # df = prd.merge_slow_csv_and_eddypro(iStation, slow_df, eddy_df, mergedCsvOutDir)
    # df.to_csv("C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/test.csv") # TODO change once debug over


# for iStation in eddyCovStations:
    
#     df = pd.read_csv("C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/test.csv",index_col=0)
#     iStation = "Reservoir"
#     # Perform gap filling
#     prd.gap_fill(iStation,df,mergedCsvOutDir,gapfillConfigDir)

#     # var_to_fill = "LE"
#     # if (iStation == "Berge") | (iStation == "Foret_ouest") | (iStation == "Foret_est"):
#     #     met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50, "HMP45C_Sensor_temp_Avg" : 2.5,"vpd" : 500}, index=[0] )
#     # elif iStation == "Reservoir":
#     #     met_vars = pd.DataFrame({"SWUpper_Avg" : 50, "air_temperature" : 2.5}, index=[0] )
#     #     prd.flux_gap_filling(iStation,var_to_fill,met_vars,mergedCsvOutDir)

