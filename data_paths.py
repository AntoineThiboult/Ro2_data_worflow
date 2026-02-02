# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 11:26:23 2025

@author: ANTHI182
"""
from pathlib import Path
station_name_conversion = {'Berge': 'Romaine-2_reservoir_shore',
                           'Berge_precip': 'Romaine-2_reservoir_shore_precip',
                           'Foret_ouest': 'Bernard_spruce_moss_west',
                           'Foret_est': 'Bernard_spruce_moss_east',
                           'Foret_sol': 'Bernard_spruce_moss_ground',
                           'Foret_precip': 'Bernard_spruce_moss_precip',
                           'Reservoir': 'Romaine-2_reservoir_raft',
                           'Bernard_lake': 'Bernard_lake'}

rawFileDir          = Path("D:/Ro2_micromet_raw_data/Data/Raw_data/Data")
reanalysisDir       = Path("D:/Ro2_micromet_raw_data/Data/Reanalysis/")
asciiOutDir         = Path("D:/Ro2_micromet_raw_data/Data/Ascii_data")
miscDataDir         = Path("D:/Ro2_micromet_raw_data/Data/Misc/")
eddyproOutDir       = Path("D:/Ro2_micromet_processed_data/Eddypro_data/")
intermediateOutDir  = Path("D:/Ro2_micromet_processed_data/Data/Intermediate/")
finalOutDir         = Path("D:/Ro2_micromet_processed_data/Final_output/")
varNameExcelSheet   = Path("./Resources/Variable_description_full.xlsx")
eddyproConfigDir    = Path("./Config/EddyProConfig/")
gapfillConfigDir    = Path("./Config/GapFillingConfig/")
filterConfigDir     = Path("./Config/Filtering/")
gasAnalyzerConfigDir    = Path("./Config/Gas_analyzer/")
reanalysisConfigDir = Path("./Config/Reanalysis/")