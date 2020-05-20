import pandas as pd
import process_micromet as pm

# TODO add a row for units
# TODO add a fluxnet name format
# TODO handle fusion and separation of berge/reservoir & foret_est/foret_ouest

### Define paths

allStations     = ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
eddyCovStations = ["Berge","Foret_ouest","Foret_est","Reservoir"]
gapfilledStation = ["Berge","Foret_ouest","Reservoir"]

rawFileDir          = "D:/E/Ro2_micromet_raw_data/Data/"
asciiOutDir         = "D:/E/Ro2_micormet_processed_data/Ascii_data/"
eddyproOutDir       = "D:/E/Ro2_micormet_processed_data/Eddypro_data/"
eddyproConfigDir    = "D:/E/Ro2_data_worflow/Config/EddyProConfig/"
externalDataDir     = "D:/E/Ro2_micromet_raw_data/Data/External_data/"
varNameExcelTab     = "./Resources/EmpreinteVariableDescription.xlsx"
mergedCsvOutDir     = "C:/Users/anthi182/Documents/Python/Explore_mds_gf/Data/"
gapfillConfigDir    = "./Config/GapFillingConfig/"

dates = {'start':'2018-06-22','end':'2020-02-01'}

### Process external data

# Merge Hobo TidBit thermistors
pm.merge_thermistors(dates, rawFileDir, mergedCsvOutDir)

# Make Natashquan data
pm.merge_natashquan(dates, externalDataDir, mergedCsvOutDir)

# Merge data relative to reservoir provided by HQ
pm.merge_hq_reservoir(dates, externalDataDir, mergedCsvOutDir)

# Extract data from the HQ weather station
pm.merge_hq_meteo_station(dates, externalDataDir, mergedCsvOutDir)


### Process eddy covariance stations
for iStation in allStations:

    # Binary to ascii
    pm.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)

    # Merge slow data
    slow_df = pm.merge_slow_csv(iStation,asciiOutDir)

    # Rename and trim slow variables
    slow_df = pm.rename_trim_vars(iStation,varNameExcelTab,slow_df,'cs')

    if iStation in eddyCovStations:

        # Ascii to eddypro
        pm.batch_process_eddypro(iStation,asciiOutDir,eddyproConfigDir,eddyproOutDir,dates)

        # Load eddypro file
        eddy_df = pm.load_eddypro_file(iStation,eddyproOutDir)

        # Rename and trim eddy variables
        eddy_df = pm.rename_trim_vars(iStation,varNameExcelTab,eddy_df,'eddypro')

        # Merge slow and eddy data
        df = pm.merge_slow_csv_and_eddypro(iStation, slow_df, eddy_df, mergedCsvOutDir)

        # Save to csv
        df.to_csv(mergedCsvOutDir+iStation+'.csv', index=False)

    else:

        # Save to csv
        slow_df.to_csv(mergedCsvOutDir+iStation+'.csv', index=False)


for iStation in gapfilledStation:

    df = pd.read_csv(mergedCsvOutDir+iStation+'.csv', low_memory=False)

    # Handle special cases and errors
    df = pm.handle_exception(iStation, df,mergedCsvOutDir, varNameExcelTab)

    # # Perform gap filling
    df = pm.gap_fill(iStation,df,mergedCsvOutDir,gapfillConfigDir)

    # # Save to csv
    df.to_csv(mergedCsvOutDir+iStation+'_gf.csv')

