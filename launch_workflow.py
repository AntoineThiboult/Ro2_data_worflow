import os
from joblib import Parallel, delayed
import process_ro2_data as prd

listStations    = ["Berge","Reservoir","Foret_ouest","Foret_est","Foret_sol"]
rawFileDir      =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Raw_data") 
asciiOutDir     =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Ascii_data")

#% Bin to ascii
Parallel(n_jobs=5)(delayed(prd.convert_CSbinary_to_csv)(iStation,rawFileDir,asciiOutDir) for iStation in listStations)    
#for iStation in listStations:
#    prd.convert_CSbinary_to_csv(iStation,rawFileDir,asciiOutDir)

   
#% Ascii to eddypro
eddyproOutDir   ="C:/Users/anthi182/Desktop/Data_for_automatization/Eddypro_data/"
eddyproConfig   ="C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/EddyProConfig/"
eddyproMetaData ="C:/Users/anthi182/Documents/GitHub/Ro2_data_worflow/EddyProConfig/"
asciiOutDir     ="C:/Users/anthi182/Desktop/Data_for_automatization/Ascii_data/"
Parallel(n_jobs=5)(delayed(prd.batch_process_eddypro)(iStation,asciiOutDir,eddyproConfig,eddyproMetaData,eddyproOutDir) for iStation in listStations) 
#for iStation in listStations:        
#    prd.batch_process_eddypro(iStation,asciiOutDir,eddyproConfig,eddyproMetaData,eddyproOutDir)