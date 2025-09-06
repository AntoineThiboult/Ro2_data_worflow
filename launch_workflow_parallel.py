from joblib import Parallel, delayed
import process_micromet as pm
from utils import data_loader as dl, dataframe_manager as dfm
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Berge_precip","Foret_ouest","Foret_est","Foret_sol","Foret_precip","Reservoir","Bernard_lake"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir","Bernard_lake"]
gapfilledStation =  ["Bernard_lake","Water_stations","Forest_stations"]

station_name_conversion = {'Berge': 'Romaine-2_reservoir_shore',
                           'Berge_precip': 'Romaine-2_reservoir_shore_precip',
                           'Foret_ouest': 'Bernard_spruce_moss_west',
                           'Foret_est': 'Bernard_spruce_moss_east',
                           'Foret_sol': 'Bernard_spruce_moss_ground',
                           'Foret_precip': 'Bernard_spruce_moss_precip',
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


def parallel_function_0(dates, rawFileDir, miscDataDir,
                        intermediateOutDir, finalOutDir):

    # Merge Hobo TidBit thermistors
    df1 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-1', dates, rawFileDir)
    pm.thermistors.save(df1,'Romaine-2_reservoir_thermistor_chain-1', finalOutDir)
    df2 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-2', dates, rawFileDir)
    pm.thermistors.save(df2,'Romaine-2_reservoir_thermistor_chain-2', finalOutDir)
    df = pm.thermistors.average(df1, df2)
    df = pm.thermistors.gap_fill(df)
    df = pm.thermistors.add_ice_phenology(df, miscDataDir+'Romaine-2_reservoir_ice_phenology')
    df = pm.thermistors.compute_energy_storage(df)
    pm.thermistors.save(df,'Romaine-2_reservoir_thermistor_chain', finalOutDir)
    df = pm.thermistors.list_merge_filter('Bernard_lake_thermistor_chain', dates, rawFileDir)
    df = pm.thermistors.gap_fill(df)
    df = pm.thermistors.add_ice_phenology(df, miscDataDir+'Bernard_lake_ice_phenology')
    df = pm.thermistors.compute_energy_storage(df)
    pm.thermistors.save(df,'Bernard_lake_thermistor_chain', finalOutDir)
    # Perform ERA5 extraction and handling
    for iStation in gapfilledStation:
        reanalysis_config = dl.yaml_file(reanalysisConfigDir, iStation)
        pm.reanalysis.retrieve( reanalysis_config['era5-land'], dates, reanalysisDir)
        pm.reanalysis.retrieve( reanalysis_config['era5'], dates, reanalysisDir)


def parallel_function_1(iStation, station_name_conversion, rawFileDir, asciiOutDir, eddyproOutDir,
                        eddyproConfigDir, finalOutDir, varNameExcelSheet):

    # Binary to ascii
    pm.convert_CSbinary_to_csv(station_name_conversion[iStation],iStation,
                               rawFileDir,asciiOutDir)
    # Correct raw concentrations
    if iStation in eddyCovStations:
        pm.correct_raw_concentrations(iStation,asciiOutDir,gasAnalyzerConfigDir,False)
    # Rotate wind
    if iStation == 'Reservoir':
        pm.rotate_wind(iStation,asciiOutDir)
    # List slow files
    slow_files = dfm.list_files(iStation, '*slow.csv', asciiOutDir)
    # Create reference dataframe and merge slow files
    df = dfm.create(dates)
    df = dfm.merge_files(df,slow_files,'TOA5')
    # Rename and trim slow variables
    db_name_map = pm.names.map_db_names(iStation, varNameExcelSheet, 'cs')
    df = pm.names.rename_trim(iStation, df, db_name_map)

    if iStation in eddyCovStations:

        # Ascii to eddypro
        pm.eddypro.run(iStation,asciiOutDir,eddyproConfigDir,
                       eddyproOutDir,dates)
        # List EddyPro files
        eddypro_files = dfm.list_files(iStation, '*full_output*.csv', eddyproOutDir)
        # Create reference dataframe and merge EddyPro files
        eddy_df = dfm.create(dates)
        eddy_df = dfm.merge_files(eddy_df,eddypro_files,'EddyPro')
        # Rename and trim eddy variables
        db_name_map = pm.names.map_db_names(iStation, varNameExcelSheet, 'eddypro')
        eddy_df = pm.rename_trim(iStation, df, db_name_map)
        # Merge slow and eddy data
        df = dfm.merge(df,eddy_df)

    dfm.save(df,intermediateOutDir,iStation)


def parallel_function_2(iStation, intermediateOutDir):

    # Load csv
    df = dl.csv(intermediateOutDir+iStation)
    # Handle exceptions
    df = pm.handle_exception(iStation,df)
    # Filter data
    df = pm.filters.apply_all(iStation,df,filterConfigDir,intermediateOutDir)
    # Save to csv
    dfm.save(df,finalOutDir,iStation)
    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,filterConfigDir,
                                      reanalysisDir ,intermediateOutDir)


def parallel_function_3(iStation, finalOutDir, rawFileDir,
                        gapfillConfigDir, miscDataDir, reanalysisDir, varNameExcelSheet):

    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,rawFileDir,
                                   finalOutDir, miscDataDir, varNameExcelSheet)

    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,filterConfigDir,
                                      reanalysisDir ,intermediateOutDir)

    # Perform gap filling
    df = pm.gap_fill_slow_data.gap_fill_meteo(
        iStation,df,intermediateOutDir,gapfillConfigDir)
    df = pm.gap_fill_slow_data.gap_fill_radiation(
        iStation,df,intermediateOutDir,gapfillConfigDir)
    df = pm.gap_fill_slow_data.custom_operation(
        iStation,df,gapfillConfigDir)
    df = pm.gap_fill_flux.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Compute storage terms
    df = pm.compute_storage_flux(iStation,df)

    if iStation == 'Forest_stations':

        # Correct for energy balance
        df = pm.correct_energy_balance(df)

        # Filter data
        df = pm.filters.apply_all(iStation,df,filterConfigDir,finalOutDir)

        # Perform gap filling
        df = pm.gap_fill_flux.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


def parallel_function_4(iStation, finalOutDir):
    df = pd.read_csv(finalOutDir+iStation+'.csv')
    fp = pm.footprint.compute(df)
    pm.footprint.dump(iStation,fp,finalOutDir)

########### Process stations ############

parallel_function_0(dates, rawFileDir, miscDataDir,
                        intermediateOutDir, finalOutDir)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_1)(
        iStation, station_name_conversion, rawFileDir, asciiOutDir,
        eddyproOutDir, eddyproConfigDir,
        finalOutDir, varNameExcelSheet)for iStation in CampbellStations)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_2)(
        iStation, intermediateOutDir)for iStation in CampbellStations)

Parallel(n_jobs=len(gapfilledStation))(delayed(parallel_function_3)(
        iStation, finalOutDir, rawFileDir, gapfillConfigDir, miscDataDir,
        reanalysisDir, varNameExcelSheet)for iStation in gapfilledStation)

Parallel(n_jobs=len(eddyCovStations))(delayed(parallel_function_4)(
        iStation, finalOutDir)for iStation in eddyCovStations)