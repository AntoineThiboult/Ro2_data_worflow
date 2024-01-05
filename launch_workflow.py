import process_micromet as pm
from utils import data_loader as dl
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir","Bernard_lake"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir","Bernard_lake"]
gapfilledStation =  ["Bernard_lake","Water_stations","Forest_stations"]

station_name_conversion = {'Berge': 'Romaine-2_reservoir_shore',
                           'Foret_ouest': 'Bernard_spruce_moss_west',
                           'Foret_est': 'Bernard_spruce_moss_east',
                           'Foret_sol': 'Bernard_spruce_moss_ground',
                           'Reservoir': 'Romaine-2_reservoir_raft',
                           'Bernard_lake': 'Bernard_lake'}

rawFileDir          = "D:/Ro2_micromet_raw_data/Data/"
reanalysisDir       = "D:/Ro2_micromet_raw_data/Data/Reanalysis/"
asciiOutDir         = "D:/Ro2_micromet_processed_data/Ascii_data/"
eddyproOutDir       = "D:/Ro2_micromet_processed_data/Eddypro_data/"
miscDataDir         = "D:/Ro2_micromet_raw_data/Data/Misc/"
intermediateOutDir  = "D:/Ro2_micromet_processed_data/Intermediate_output/"
finalOutDir         = "D:/Ro2_micromet_processed_data/Final_output/"
varNameExcelSheet   = "./Resources/Variable_description_full.xlsx"
eddyproConfigDir    = "./Config/EddyProConfig/"
gapfillConfigDir    = "./Config/GapFillingConfig/"
filterConfigDir     = "./Config/Filtering/"
gasAnalyzerConfigDir    = "./Config/Gas_analyzer/"
reanalysisConfigDir = "./Config/Reanalysis/"

dates = {'start':'2018-06-25','end':'2022-10-01'}


# Merge Hobo TidBit thermistors
df1 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-1', dates, rawFileDir)
pm.thermistors.save(df1,'Romaine-2_reservoir_thermistor_chain-1', finalOutDir)
df2 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-2', dates, rawFileDir)
pm.thermistors.save(df2,'Romaine-2_reservoir_thermistor_chain-2', finalOutDir)
df = pm.thermistors.average(df1, df2)
df = pm.thermistors.gap_fill(df)
pm.thermistors.save(df,'Romaine-2_reservoir_thermistor_chain', finalOutDir)
df = pm.thermistors.list_merge_filter('Bernard_lake_thermistor_chain', dates, rawFileDir)
df = pm.thermistors.gap_fill(df)
pm.thermistors.save(df,'Bernard_lake_thermistor_chain', finalOutDir)
# Perform ERA5 extraction and handling
pm.reanalysis.retrieve( dl.yaml_file(
    reanalysisConfigDir,'era5_land'), dates,reanalysisDir)
pm.reanalysis.retrieve( dl.yaml_file(
    reanalysisConfigDir,'era5'), dates,reanalysisDir)


for iStation in CampbellStations:

    # Binary to ascii
    pm.convert_CSbinary_to_csv(station_name_conversion[iStation],rawFileDir,asciiOutDir)
    # Correct raw concentrations
    if iStation in eddyCovStations:
        pm.correct_raw_concentrations(iStation,asciiOutDir,gasAnalyzerConfigDir,False)
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
                                   finalOutDir, miscDataDir, varNameExcelSheet)

    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,filterConfigDir,
                                      reanalysisDir,intermediateOutDir)

    # Perform gap filling
    df = pm.gap_fill_slow_data.gap_fill_meteo(
        iStation,df,intermediateOutDir,gapfillConfigDir)
    df = pm.gap_fill_slow_data.gap_fill_radiation(
        iStation,df,intermediateOutDir,gapfillConfigDir)
    df = pm.gap_fill_slow_data.custom_operation(
        iStation,df,gapfillConfigDir)
    df = pm.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Compute storage terms
    df = pm.compute_storage_flux(iStation,df)

    if iStation == 'Forest_stations':
        # Correct for energy balance
        df = pm.correct_energy_balance(df)

        # Filter data
        df = pm.filters.apply_all(iStation,df,filterConfigDir,finalOutDir)

        # Perform gap filling
        df = pm.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)