from joblib import Parallel, delayed
import process_micromet as pm
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir","Bernard_lake"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir","Bernard_lake"]
gapfilledStation =  ["Bernard_lake","Water_stations","Forest_stations"]

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
rawConcConfigDir    = "./Config/Raw_gas_concentration/"

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
    pm.thermistors.save(df,'Romaine-2_reservoir_thermistor_chain', finalOutDir)
    df = pm.thermistors.list_merge_filter('Bernard_lake_thermistor_chain', dates, rawFileDir)
    df = pm.thermistors.gap_fill(df)
    pm.thermistors.save(df,'Bernard_lake_thermistor_chain', finalOutDir)
    # Perform ERA5 extraction and handling
    pm.reanalysis.retrieve_ERA5land(dates,reanalysisDir)


def parallel_function_1(iStation, rawFileDir, asciiOutDir, eddyproOutDir,
                        eddyproConfigDir, finalOutDir, varNameExcelSheet):

    # Binary to ascii
    pm.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)
    # Correct raw concentrations
    pm.correct_raw_concentrations(iStation,asciiOutDir,rawConcConfigDir,False)
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


def parallel_function_2(iStation, intermediateOutDir):

    # Load csv
    df = pd.read_csv(intermediateOutDir+iStation+'.csv')
    # Handle exceptions
    df = pm.handle_exception(iStation,df)
    # Filter data
    df = pm.filters.apply_all(iStation,df,filterConfigDir,intermediateOutDir)
    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


def parallel_function_3(iStation, finalOutDir, rawFileDir,
                        gapfillConfigDir, miscDataDir, reanalysisDir, varNameExcelSheet):

    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,rawFileDir,
                                   finalOutDir, miscDataDir, varNameExcelSheet)

    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,filterConfigDir,
                                      reanalysisDir ,intermediateOutDir)

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
        df = pm.gap_fill_flux(iStation,df,gapfillConfigDir)

    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


########### Process stations ############

parallel_function_0(dates, rawFileDir, miscDataDir,
                        intermediateOutDir, finalOutDir)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_1)(
        iStation, rawFileDir, asciiOutDir,
        eddyproOutDir, eddyproConfigDir,
        finalOutDir, varNameExcelSheet)for iStation in CampbellStations)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_2)(
        iStation, intermediateOutDir)for iStation in CampbellStations)

Parallel(n_jobs=len(gapfilledStation))(delayed(parallel_function_3)(
        iStation, finalOutDir, rawFileDir, gapfillConfigDir, miscDataDir,
        reanalysisDir, varNameExcelSheet)for iStation in gapfilledStation)