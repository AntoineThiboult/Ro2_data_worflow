import process_micromet as pm
import data_paths as path
from utils import data_loader as dl, dataframe_manager as dfm
import pandas as pd

### Define paths

CampbellStations =  ["Berge","Berge_precip","Foret_ouest","Foret_est","Foret_sol","Foret_precip","Reservoir","Bernard_lake"]
eddyCovStations =   ["Berge","Foret_ouest","Foret_est","Reservoir","Bernard_lake"]
gapfilledStation =  ["Bernard_lake","Water_stations","Forest_stations"]

dates = {'start':'2018-06-25','end':'2022-10-01'}


# Merge Hobo TidBit thermistors
df1 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-1', dates, path.rawFileDir)
pm.thermistors.save(df1,'Romaine-2_reservoir_thermistor_chain-1', path.finalOutDir)
df2 = pm.thermistors.list_merge_filter('Romaine-2_reservoir_thermistor_chain-2', dates, path.rawFileDir)
pm.thermistors.save(df2,'Romaine-2_reservoir_thermistor_chain-2', path.finalOutDir)
df = pm.thermistors.average(df1, df2)
df = pm.thermistors.gap_fill(df)
df = pm.thermistors.add_ice_phenology(df, path.miscDataDir+'Romaine-2_reservoir_ice_phenology')
df = pm.thermistors.compute_energy_storage(df)
pm.thermistors.save(df,'Romaine-2_reservoir_thermistor_chain', path.finalOutDir)
df = pm.thermistors.list_merge_filter('Bernard_lake_thermistor_chain', dates, path.rawFileDir)
df = pm.thermistors.gap_fill(df)
df = pm.thermistors.add_ice_phenology(df, path.miscDataDir+'Bernard_lake_ice_phenology')
df = pm.thermistors.compute_energy_storage(df)
pm.thermistors.save(df,'Bernard_lake_thermistor_chain', path.finalOutDir)

# Perform ERA5 extraction and handling
for iStation in gapfilledStation:
    reanalysis_config = dl.yaml_file(path.reanalysisConfigDir, iStation)
    pm.reanalysis.retrieve( reanalysis_config['era5-land'], dates, path.reanalysisDir)
    pm.reanalysis.retrieve( reanalysis_config['era5'], dates, path.reanalysisDir)

for iStation in CampbellStations:

    # Binary to ascii
    pm.convert_CSbinary_to_csv(path.station_name_conversion[iStation],iStation,
                               path.rawFileDir,path.asciiOutDir)
    # Correct raw concentrations
    if iStation in eddyCovStations:
        pm.correct_raw_concentrations(iStation,path.asciiOutDir,path.gasAnalyzerConfigDir,False)
    # Rotate wind
    if iStation == 'Reservoir':
        pm.rotate_wind(iStation,path.asciiOutDir)
    # List slow files
    slow_files = dfm.list_files(iStation, '*slow.csv', path.asciiOutDir)
    # Create reference dataframe and merge slow files
    df = dfm.create(dates)
    df = dfm.merge_files(df,slow_files,'TOA5')
    # Rename and trim slow variables
    db_name_map = pm.names.map_db_names(iStation, path.varNameExcelSheet, 'cs')
    df = pm.names.rename_trim(iStation, df, db_name_map)

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


for iStation in CampbellStations:
    # Load csv
    df = dl.csv(path.intermediateOutDir+iStation)
    # Handle exceptions
    df = pm.handle_exception(iStation,df)
    # Filter data
    df = pm.filters.apply_all(iStation,df,path.filterConfigDir,path.intermediateOutDir)
    # Save to csv
    dfm.save(df,path.finalOutDir,iStation)
    # Format reanalysis data
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,path.filterConfigDir,
                                      path.reanalysisDir,path.intermediateOutDir)


for iStation in gapfilledStation:
    # Merge the eddy covariance together (water/forest)
    df = pm.merge_eddycov_stations(iStation,path.rawFileDir,
                                   path.finalOutDir, path.miscDataDir, path.varNameExcelSheet)

    # Format reanalysis data for gapfilling
    pm.reanalysis.netcdf_to_dataframe(dates,iStation,path.filterConfigDir,
                                      path.reanalysisDir,path.intermediateOutDir)

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


for iStation in eddyCovStations:
    df = pd.read_csv(path.finalOutDir+iStation+'.csv')
    fp = pm.footprint.compute(df)
    pm.footprint.dump(iStation,fp,path.finalOutDir)