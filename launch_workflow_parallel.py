from joblib import Parallel, delayed
import process_micromet as pm
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Foret_ouest","Foret_est","Foret_sol","Reservoir"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir"]
gapfilledStation =  ["Water_stations","Forest_stations"]

rawFileDir          = "F:/Ro2_micromet_raw_data/Data/"
asciiOutDir         = "F:/Ro2_micormet_processed_data/Ascii_data/"
eddyproOutDir       = "F:/Ro2_micormet_processed_data/Eddypro_data/"
externalDataDir     = "F:/Ro2_micromet_raw_data/Data/External_data_and_misc/"
intermediateOutDir  = "F:/Ro2_micormet_processed_data/Intermediate_output/"
finalOutDir         = "F:/Ro2_micormet_processed_data/Final_output/"
varNameExcelSheet   = "./Resources/Variable_description_full.xlsx"
eddyproConfigDir    = "./Config/EddyProConfig/"
gapfillConfigDir    = "./Config/GapFillingConfig/"

dates = {'start':'2018-06-25','end':'2021-10-01'}


def parallel_function_0(dates, rawFileDir, externalDataDir,
                        intermediateOutDir, finalOutDir):

    # Merge Hobo TidBit thermistors
    pm.merge_thermistors(dates,rawFileDir,finalOutDir)
    # Make Natashquan data
    pm.merge_natashquan(dates,externalDataDir,finalOutDir)
    # Merge data relative to reservoir provided by HQ
    pm.merge_hq_reservoir(dates,externalDataDir,finalOutDir)
    # Extract data from the HQ weather station
    pm.merge_hq_meteo_station(dates,externalDataDir,finalOutDir)
    # Perform ERA5 extraction and handling
    pm.retrieve_ERA5land(dates,rawFileDir)
    pm.handle_netcdf(dates,rawFileDir,intermediateOutDir)


def parallel_function_1(iStation, rawFileDir, asciiOutDir, eddyproOutDir,
                        eddyproConfigDir, finalOutDir, varNameExcelSheet):

    # Binary to ascii
    pm.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)
    # Merge slow data
    slow_df = pm.merge_slow_csv(dates,iStation,asciiOutDir)
    # Rename and trim slow variables
    slow_df = pm.rename_trim_vars(iStation,varNameExcelSheet,slow_df,'cs')

    if iStation in eddyCovStations:

        # Ascii to eddypro
        pm.batch_process_eddypro(iStation,asciiOutDir,eddyproConfigDir,
                                 eddyproOutDir,dates)
        # Load eddypro file
        eddy_df = pm.load_eddypro_file(iStation,eddyproOutDir)
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
    df = pm.filter_data(iStation,df,intermediateOutDir)
    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


def parallel_function_3(iStation, finalOutDir, rawFileDir,
                        gapfillConfigDir, varNameExcelSheet):

    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,rawFileDir,
                                    finalOutDir,varNameExcelSheet)

    # Perform gap filling
    df = pm.gap_fill_slow_data(iStation,df,intermediateOutDir)
    df = pm.gap_fill_flux(iStation,df,finalOutDir,gapfillConfigDir)

    # Compute storage terms
    df = pm.compute_storage_flux(iStation,df)

    # Correct for energy balance
    df = pm.correct_energy_balance(df)

    # Save to csv
    df.to_csv(finalOutDir+iStation+'.csv',index=False)


########### Process stations ############

parallel_function_0(dates, rawFileDir, externalDataDir,
                        intermediateOutDir, finalOutDir)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_1)(
        iStation, rawFileDir, asciiOutDir,
        eddyproOutDir, eddyproConfigDir,
        finalOutDir, varNameExcelSheet)for iStation in CampbellStations)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_2)(
        iStation, intermediateOutDir)for iStation in CampbellStations)

Parallel(n_jobs=len(gapfilledStation))(delayed(parallel_function_3)(
        iStation, finalOutDir, rawFileDir, gapfillConfigDir,
        varNameExcelSheet)for iStation in gapfilledStation)