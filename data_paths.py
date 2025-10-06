# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 11:26:23 2025

@author: ANTHI182
"""

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