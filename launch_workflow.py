import pandas as pd
#from joblib import Parallel, delayed
import process_ro2_data as prd

# TODO deal with start/end dates, especially in merge_therm and eddypro
# TODO add  condition on convert_CSbinary_to_csv to avoid re-conversion (check if dir exist)g
# TODO add a row for units
# TODO add verbose
# TODO add despike 
# TODO add docstring

### Define paths

# allStations     = ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
# eddyCovStations = ["Berge","Foret_ouest","Foret_est","Reservoir"]

# rawFileDir          = "E:/Ro2_micromet_raw_data/Data"
# asciiOutDir         = "C:/Users/anthi182/Desktop/Micromet_data/Ascii_data/"
# eddyproOutDir       = "C:/Users/anthi182/Desktop/Micromet_data/Eddypro_data/"
# eddyproConfigDir    = "E:/Ro2_data_worflow/Config/EddyProConfig/"
# mergedCsvOutDir     = "C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/"
# gapfillConfigDir    = "E:/Ro2_data_worflow/Config/GapFillingConfig/"

# dates = {'start':'2018-06-01','end':'2020-02-01'}


allStations     = ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
eddyCovStations = ["Berge","Foret_ouest","Foret_est","Reservoir"]

rawFileDir          = "C:/Users/anthi182/Desktop/Thermistors/"
# rawFileDir          = "E:/Ro2_micromet_raw_data/Data"
asciiOutDir         = "C:/Users/anthi182/Desktop/Micromet_data/Ascii_data/"
eddyproOutDir       = "C:/Users/anthi182/Desktop/Micromet_data/Eddypro_data/"
eddyproConfigDir    = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/Config/EddyProConfig/"
mergedCsvOutDir     = "C:/Users/anthi182/Desktop/Micromet_data/Merged_csv/"
gapfillConfigDir    = "C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/Config/GapFillingConfig/"

dates = {'start':'2018-06-01','end':'2019-11-01'}

allStations = ["Berge"]
eddyCovStations  = ["Berge"]

### Process stations

# # Merge Hobo TidBit thermistors
# prd.merge_thermistors(dates, rawFileDir, mergedCsvOutDir)

for iStation in allStations:

# #     # Binary to ascii
# #     # prd.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)

#     # Merge slow data
    slow_df = prd.merge_slow_csv(iStation,asciiOutDir,dates)
    
#     # Rename and trim slow variables
    slow_df = prd.rename_trim_vars(iStation,slow_df)
    
#     if iStation in eddyCovStations:

#         # Ascii to eddypro
#         # prd.batch_process_eddypro(iStation,asciiOutDir,eddyproConfigDir,eddyproOutDir,dates)
        
#         # Load eddypro file
    eddy_df = prd.load_eddypro_file(iStation,eddyproOutDir)
        
#         # Rename and trim eddy variables
    eddy_df = prd.rename_trim_vars(iStation,eddy_df)
    
#         # Merge slow and eddy data
    df = prd.merge_slow_csv_and_eddypro(iStation, slow_df, eddy_df, mergedCsvOutDir)
        
#         # Save to csv
    df.to_csv(mergedCsvOutDir+iStation+'.csv', index=False)
        
# #     else:
        
# #         # Save to csv
#     # slow_df.to_csv(mergedCsvOutDir+iStation+'.csv', index=False)
        
        
for iStation in eddyCovStations:
    
    df = pd.read_csv(mergedCsvOutDir+iStation+'.csv', low_memory=False)

    # # Perform gap filling
    df = prd.gap_fill(iStation,df,mergedCsvOutDir,gapfillConfigDir)
    
    # # Save to csv
    df.to_csv(mergedCsvOutDir+iStation+'_gf.csv')