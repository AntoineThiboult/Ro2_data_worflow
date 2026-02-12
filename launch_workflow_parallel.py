from joblib import Parallel, delayed
import process_micromet as pm
import data_paths as path
from utils import data_loader as dl, dataframe_manager as dfm

### Define paths

CampbellStations =  ["Berge","Berge_precip","Foret_ouest","Foret_est","Foret_sol","Foret_precip","Reservoir","Bernard_lake"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir","Bernard_lake"]
gapfilledStation =  ["Bernard_lake","Water_stations","Forest_stations"]

dates = {'start':'2018-06-25','end':'2025-12-15'}


def parallel_function_0(dates, path):

    # Merge Hobo TidBit thermistors
    df1 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-1', dates, path.rawFileDir)
    pm.thermistors.save(df1,'Romaine-2_reservoir_thermistor_chain-1', path.finalOutDir)
    df2 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-2', dates, path.rawFileDir)
    pm.thermistors.save(df2,'Romaine-2_reservoir_thermistor_chain-2', path.finalOutDir)
    df = pm.thermistors.average(df1, df2)
    df = pm.thermistors.gap_fill(df)
    df = pm.thermistors.add_ice_phenology(df, path.miscDataDir.joinpath('Romaine-2_reservoir_ice_phenology'))
    df = pm.thermistors.compute_energy_storage(df)
    pm.thermistors.save(df,'Romaine-2_reservoir_thermistor_chain', path.finalOutDir)
    df = pm.thermistors.list_merge_filter('Bernard_lake_thermistor_chain', dates, path.rawFileDir)
    df = pm.thermistors.gap_fill(df)
    df = pm.thermistors.add_ice_phenology(df, path.miscDataDir.joinpath('Bernard_lake_ice_phenology'))
    df = pm.thermistors.compute_energy_storage(df)
    pm.thermistors.save(df,'Bernard_lake_thermistor_chain', path.finalOutDir)
    # Perform ERA5 extraction and handling
    for iStation in gapfilledStation:
        reanalysis_config = dl.yaml_file(path.reanalysisConfigDir, iStation)
        pm.reanalysis.retrieve( reanalysis_config['era5-land'], dates, path.reanalysisDir)
        pm.reanalysis.retrieve( reanalysis_config['era5'], dates, path.reanalysisDir)


def parallel_function_1(iStation, path):

    # Binary to ascii
    unconverted_files = pm.csbinary_to_csv.find_unconverted_files(path.station_name_conversion[iStation],iStation,
                                path.rawFileDir,path.asciiOutDir)
    pm.csbinary_to_csv.convert(iStation, path.asciiOutDir, unconverted_files)
    # List slow files
    slow_files = dfm.list_files(iStation, '*slow.csv', path.asciiOutDir)
    # Create reference dataframe and merge slow files
    df = dfm.create(dates)
    df = dfm.merge_files(df,slow_files,'TOA5')
    # Rename and trim slow variables
    db_name_map = pm.names.map_db_names(iStation, path.varNameExcelSheet, 'cs')
    df = pm.names.rename_trim(iStation, df, db_name_map)
    df = pm.filters.remove_by_variable_and_date(df, path.filterConfigDir, f"{iStation}_erroneous_variables")

    # Correct raw concentrations
    if iStation in eddyCovStations:
        gas_analyzer_info = dl.yaml_file(path.gasAnalyzerConfigDir, f"{iStation}_gas_analyzer")
        corr_coeff = pm.gas_analyzer.get_correction_coeff(df,gas_analyzer_info,iStation)
        uncorrected_files = pm.gas_analyzer.find_uncorrected_files(path.asciiOutDir.joinpath(iStation))
        pm.gas_analyzer.correct_densities(iStation, corr_coeff, uncorrected_files)
    # Rotate wind
    if iStation == 'Reservoir':
        unrotated_files = pm.sonic.find_unrotated_files(path.asciiOutDir.joinpath(iStation))
        pm.sonic.rotate(iStation,unrotated_files)



    if iStation in eddyCovStations:

        # Ascii to eddypro
        pm.eddypro.run(iStation,path.asciiOutDir,path.eddyproConfigDir,
                       path.eddyproOutDir,dates)
        # List EddyPro files
        eddypro_files = dfm.list_files(iStation, '*full_output*.csv', path.eddyproOutDir)
        # Create reference dataframe and merge EddyPro files
        eddy_df = dfm.create(dates)
        eddy_df = dfm.merge_files(eddy_df,eddypro_files,'EddyPro')
        # Rename and trim eddy variables
        db_name_map = pm.names.map_db_names(iStation, path.varNameExcelSheet, 'eddypro')
        eddy_df = pm.names.rename_trim(iStation, eddy_df, db_name_map)
        # Merge slow and eddy data
        df = dfm.merge(df,eddy_df)

    dfm.save(df,path.intermediateOutDir,iStation)


def parallel_function_2(iStation, path):

    # Load csv
    df = dl.csv(path.intermediateOutDir.joinpath(iStation))
    # Handle exceptions
    df = pm.handle_exception(iStation,df)
    # Filter data
    df = pm.filters.apply_all(iStation,df,path.filterConfigDir,path.intermediateOutDir)
    # Save to csv
    dfm.save(df,path.finalOutDir,iStation)
    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,path.filterConfigDir,
                                      path.reanalysisDir ,path.intermediateOutDir)


def parallel_function_3(iStation, path):

    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,path.rawFileDir,
                                   path.finalOutDir, path.miscDataDir, path.varNameExcelSheet)

    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,path.filterConfigDir,
                                      path.reanalysisDir ,path.intermediateOutDir)

    # Perform gap filling
    df = pm.gap_fill_slow_data.gap_fill_meteo(
        iStation,df,path.intermediateOutDir,path.gapfillConfigDir)
    df = pm.gap_fill_slow_data.gap_fill_radiation(
        iStation,df,path.intermediateOutDir,path.gapfillConfigDir)
    df = pm.gap_fill_slow_data.custom_operation(
        iStation,df,path.gapfillConfigDir)
    df = pm.gap_fill_flux.gap_fill_flux(iStation,df,path.gapfillConfigDir)

    # Compute storage terms
    df = pm.compute_storage_flux(iStation,df)

    # Correct for energy balance
    if iStation == 'Forest_stations': # Land type station
        df = pm.correct_energy_balance(df)
    else: # Water body type station
        df = pm.correct_energy_balance(df, 1.34)

    # Filter data
    df = pm.filters.apply_all(iStation,df,path.filterConfigDir,path.finalOutDir)

    # Perform gap filling
    df = pm.gap_fill_flux.gap_fill_flux(iStation,df,path.gapfillConfigDir)

    # Save to csv
    dfm.save(df,path.finalOutDir,iStation)


def parallel_function_4(iStation, path):
    df = dl.csv(path.finalOutDir.joinpath(iStation))
    fp = pm.footprint.compute(df)
    pm.footprint.dump(iStation,fp,path.finalOutDir)

########### Process stations ############

parallel_function_0(dates, path)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_1)(
    iStation, path)for iStation in CampbellStations)

Parallel(n_jobs=len(CampbellStations))(delayed(parallel_function_2)(
        iStation, path)for iStation in CampbellStations)

Parallel(n_jobs=len(gapfilledStation))(delayed(parallel_function_3)(
        iStation, path)for iStation in gapfilledStation)

Parallel(n_jobs=len(eddyCovStations))(delayed(parallel_function_4)(
        iStation, path)for iStation in eddyCovStations)