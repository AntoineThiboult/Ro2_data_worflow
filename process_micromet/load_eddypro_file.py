# -*- coding: utf-8 -*-
# TODO chose between glob and os.listdir
import os
import pandas as pd
from glob import glob

def load_eddypro_file(stationName,inputDir):
    """Load the most recent EddyPro output files found in source directory and
    create a more pythonic column that contains date and time

    Parameters
    ----------
    stationName: name of the station
    inputDir: path of the directory that contains the EddyPro output files

    Returns
    -------
    df: a nice and tidy pandas DataFrame
    """
    print('Start loading eddy pro file for station:', stationName, '...', end='\r')
    # List eddy pro output files and select most recent one
    eddyFullOutputList = glob(inputDir+'/'+stationName+'/'+'\*full_output*.csv')
    eddyProFileToLoad = max(eddyFullOutputList, key=os.path.getctime) # Select latest file

    # Import as dataframe
    df = pd.read_csv(eddyProFileToLoad,skiprows=[0,2])

    # Create a standardized time column
    df['TIMESTAMP']=pd.to_datetime(df.date.map(str) +" "+ df.time.map(str), yearfirst=True)
    print('Done!')

    return df