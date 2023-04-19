import process_micromet as pm
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir"]
gapfilledStation =  ["Water_stations","Forest_stations"]

rawFileDir          = "D:/Ro2_micromet_raw_data/Data/"
asciiOutDir         = "D:/Ro2_micromet_processed_data/Ascii_data/"
eddyproOutDir       = "D:/Ro2_micromet_processed_data/Eddypro_data/"
externalDataDir     = "D:/Ro2_micromet_raw_data/Data/External_data_and_misc/"
intermediateOutDir  = "D:/Ro2_micromet_processed_data/Intermediate_output/"
finalOutDir         = "D:/Ro2_micromet_processed_data/Final_output/"
varNameExcelSheet   = "./Resources/Variable_description_full.xlsx"
eddyproConfigDir    = "./Config/EddyProConfig/"
gapfillConfigDir    = "./Config/GapFillingConfig/"
filterConfigDir     = "./Config/Filtering/"

dates = {'start':'2018-06-25','end':'2022-10-01'}


# Merge Hobo TidBit thermistors
pm.merge_thermistors(dates,rawFileDir,finalOutDir)
# Make Natashquan data
pm.merge_natashquan(dates,externalDataDir,finalOutDir)
# Merge data relative to reservoir provided by HQ
pm.merge_hq_reservoir(dates,externalDataDir,finalOutDir)
# Extract data from the HQ weather station
pm.merge_hq_meteo_station(dates,externalDataDir,finalOutDir)
# Perform ERA5 extraction and handling
pm.reanalysis.retrieve_ERA5land(dates,rawFileDir)
pm.reanalysis.netcdf_to_dataframe(dates,rawFileDir,intermediateOutDir)


for iStation in CampbellStations:

    # Binary to ascii
    pm.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)
    # Rotate wind
    pm.rotate_wind(iStation,asciiOutDir)
    # Merge slow data
    slow_df = pm.merge_slow_csv(dates,iStation,asciiOutDir)
    # Rename and trim slow variables
    slow_df = pm.rename_trim_vars(iStation,varNameExcelSheet,slow_df,'cs')

    if iStation in eddyCovStations:
        # Ascii to eddypro
        pm.eddypro.run(iStation,asciiOutDir,eddyproConfigDir,
                       eddyproOutDir,dates)
        # Load eddypro file
        eddy_df = pm.eddypro.merge(iStation,eddyproOutDir,dates)
        # Rename and trim eddy variables
        eddy_df = pm.rename_trim_vars(iStation,varNameExcelSheet,
                                      eddy_df,'eddypro')
        # Merge slow and eddy data
        df = pm.merge_slow_csv_and_eddypro(iStation,slow_df,eddy_df)

    else:
        # Rename Dataframe
        df = slow_df

    # Save to csv
    df.to_csv(intermediateOutDir+iStation+'.csv',index=False)


for iStation in CampbellStations:
    # Load csv
    df = pd.read_csv(intermediateOutDir+iStation+'.csv')
    # Handle exceptions
    df = pm.handle_exception(iStation,df)
    # Filter data
    df = pm.filters.apply_all(iStation,df,filterConfigDir,intermediateOutDir)
    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


for iStation in gapfilledStation:
    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,rawFileDir,
                                   finalOutDir,varNameExcelSheet)

    # Perform gap filling
    df = pm.gap_fill_slow_data.gap_fill_meteo(iStation,df,intermediateOutDir)
    df = pm.gap_fill_slow_data.gap_fill_radiation(iStation,df,intermediateOutDir)
    df = pm.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Compute storage terms
    df = pm.compute_storage_flux(iStation,df)

    if iStation == 'Forest_stations':
        # Correct for energy balance
        df = pm.correct_energy_balance(df)

        # Filter data
        df = pm.filters.apply_all(iStation,df,filterConfigDir,finalOutDir)

        # Perform gap filling
        df = pm.gap_fill_flux(iStation,df,finalOutDir,gapfillConfigDir)

    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)