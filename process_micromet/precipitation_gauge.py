import numpy as np
import pandas as pd
from datetime import timedelta
import warnings
warnings.simplefilter("ignore", category=RuntimeWarning)


def precip_intensity(precip):

    """
    Decumulate precipitation cumulated series

    Parameters
    ----------
    series : Numpy array
        Cumulated precipitation series

    Returns
    -------
    precip_int : Numpy array
        Precipitation intensity
    """
    precip_int = np.hstack([[0], np.diff(precip)])
    return precip_int


def precip_cum(dates, series, return_time=False):
    """
    Call the NAE SEG algorithm to produce a clean cumulative precipitation series


    Parameters
    ----------
    dates : datetime array
        Datetime format dates associated with the time series
    series : Numpy array
        Raw cumulative precipitation time series
    return_time : Bool
        Controls the output. If true, returns time and precipitation,
        otherwise, precipitation only.

    Returns
    -------
    precip : Numpy array
        Cumulated precipitation
    t: datetime64 array / DatetimeIndex. Optional (return_time)
    """
    t, precip, evap = NAF_SEG(dates, series)
    if return_time:
        return t, precip
    else:
        return precip


def NAF_SEG(xt, xRawCumPcp, intPcpTh=0.001, nRecsPerDay=48, nWindowsPerDay=3, fDetect = 'All'):
    '''
    VERSION 1.2 20200420
    PYTHON version 1.0
    ***********************************************************************
    Written by A. Barr & Amber Ross, Environment and Climate Change Canada,
    Climate Research Division, Saskatoon, Canada, 8 Dec 2018
    Transfert into Python by Jean-Benoit Madore Universite de Sherbrooke
    Sherbrooke, Quebec, Canada, April 2022
    ***********************************************************************

    The Segmented Neutral Aggregating Filter (NAF_SEG)
    filters a cumulative precipitation time series xRawCumPcp
    24 hours at a time (within overlapping moving windows)
    using the brute-force NAF filter (Smith et al., 2019)
    and produces an estimate of evaporation on days without
    precipitation.


    Inputs:

    xt: datetime array : datetime format dates associated with the time series
    xRawCumPcp: numpy array: raw cumulative precipitation time series

    intPcpTh: float: desired minimum interval precipitation P*
    (intPcpTh=0.001 is recommended)

    nRecsPerDay: int
        Number of measurements per day example: hourly data -> nRecsPerDay=24
    nWindowsPerDay: int
        Number of overlapping moving windows per day - must be a factor
        of nRecsPerDay (nWindowsPerDay=3 is recommended)
    output_type: string
        Type of output variable:
            -> 'dataframe' (default): panda dataframe
            -> 'dictionary': python dictionary
    fDetect: string
        Flag used to consider precip or evap, default= 'All'
            ->'Any' Needs one flag per timestep to consider precip or evap
            -> 'All' Needs all windows flags per timestep to consider precip or evap
            -> 'Half'Needs Half windows flags per timestep to consider precip or evap

    Outputs:
    out_NAF_SEG: pandas datafarame/dictionary with columns/keys:
       -> t: as xt but with complete days of nRecsPerDay. Returned
          t will be in datenum format
       -> cumPcpFilt: filtered cumulative precipitation time series vector
       -> cumEvap: inferred cumulative evaporation time series vector

    The filtering is done using a brute-force algorithm NAF
    that identifies small or negative changes (below intPcpTh)
    then transfers them to neighbouring positive changes
    thus aggregating all changes to values above intPcpTh.

    Note that the precipitation on the first and last days of the time series
    are adversely impacted unless the bucket weight time series is "padded"
    with 1 day of fictitious precipitation at the beginning and end of the
    time series. To remedy this, add 1 full day of zeros to the beginning of
    the data and one full day of max bucket weight values to the end of the
    data. This will allow the algorithm to make a precipitation estimate for
    those two days.

    Revisions:
    4 Oct 2019 (Craig Smith) Replaced the call to the function PcpFiltPosTh
                with the call to NAF which is the version published in Smith
                et al. (2019).
                Fixed the code so that t can be passed to the function in
                either datenum or datetime format. datetime is converted to
                datenum
    20 Apr 2020 (Amber Ross) Removed un-used mArrays and sArrays structures
                from the function exports. Cleaned up code and comments.
    15 Apr 2022 (Jean-Benoit Madore) Adapted for python

    --------------------------------------------------------------------------
    References:
    Smith, C. D., Yang, D., Ross, A., and Barr, A.: The Environment and
    Climate Change Canada solid precipitation intercomparison data from
    Bratt's Lake and Caribou Creek, Saskatchewan, Earth Syst. Sci. Data, 11,
    1337–1347, https://doi.org/10.5194/essd-11-1337-2019, 2019.
    --------------------------------------------------------------------------

    '''

    nRecsPerBlock = nRecsPerDay/nWindowsPerDay # moving window increment
    xt = pd.DatetimeIndex(pd.to_datetime(xt))# work in pandas date format
    tDay = np.unique(xt.date) # Make sure there is no duplicates
    dt = 1/nRecsPerDay # Fraction of day to use with timedelta

    # Removing obvious corrupted data and reset the series after a discontinuity
    threshold = 100
    jumps = np.where(np.abs(np.diff(xRawCumPcp)) > threshold)[0]
    for j in jumps:
        xRawCumPcp[j+1:] = xRawCumPcp[j+1:] + xRawCumPcp[j] - xRawCumPcp[j+1]


    # Setting theorical timeseries
    t = pd.date_range(start = tDay[0]+timedelta(days=dt), end = tDay[-1], freq=str(dt)+'D')
    # lenght of timeserie
    nt = len(t)
    # create an empty series to put the cumulative precip
    cumPcpRaw = pd.Series(np.nan, index=t)
    # create an empty series to put the intensity precip
    intPcpRaw = pd.Series(np.nan, index=t)

    # map xt onto t
    ftMap = xt.intersection(t)
    # Put the raw precip in the series. Any missing time will be np.nan
    cumPcpRaw[ftMap] = xRawCumPcp
    # Find all non np.nan values
    itYaN = np.where(~np.isnan(cumPcpRaw.values))[0]

    # Discrete diffenrence of all records to deacumulate
    intPcpRaw.iloc[itYaN] = cumPcpRaw.iloc[itYaN].diff()
    # Fill first value with 0. The value was discarted during the deacumulation
    intPcpRaw.iloc[itYaN[0]] = 0

    # Create np.nan matrices to fill with filtered data
    intPcpArray=np.empty([nt,nWindowsPerDay]); # interval precipitation
    intPcpArray[:] = np.nan
    intEvapArray=np.empty([nt,nWindowsPerDay]) # interval evaporation
    intEvapArray[:] = np.nan
    flagPcpArray=np.empty([nt,nWindowsPerDay]); # interval precipitation flag
    flagPcpArray[:] = np.nan
    flagEvapArray=np.empty([nt,nWindowsPerDay]); # interval evaporation flag
    flagEvapArray[:] = np.nan

    # Iterate index from 0 to lenght of the time series minus the last day's measurements
    for itW1 in np.arange(0.0, nt+1-nRecsPerDay, nRecsPerBlock, dtype=int):

        # index of the 24h later measurment
        itW2 = itW1 + nRecsPerDay # All measurement of the day

        # indexes that are evaluated
        itW = np.arange(itW1,itW2)

        # Find data in cumPcpRaw within itW indexes
        jtYaN = np.where(~np.isnan(cumPcpRaw.iloc[itW]))[0]

        # number of valid values within the evaluated day
        ntYaN=len(jtYaN)

        # Find np.nan in cumPcpRaw within itW index
        jtNaN = np.where(np.isnan(cumPcpRaw.iloc[itW]))[0]

        if ntYaN>=2: # Case where we can apply the filter if we have more than 2 data points

            # if 24h accumulation dPcpW >= intPcpTh, filter and treat as Precip
            # if 24h accumulation dPcpW <= -intPcpTh, filter and treat as Evap
            # otherwise set both Pcp and Evap to zero.

            # Cumulative precipiation for the 24h evaluated
            dPcpW = cumPcpRaw.iloc[itW[jtYaN[-1]]] - cumPcpRaw.iloc[itW[jtYaN[0]]]

            # Case the precipitation is greater than the minimal threshold
            if dPcpW>=intPcpTh:

                # Precip detected within the window. Filter the 24h with NAF
                tmpCumPcpFilt=NAF(cumPcpRaw.iloc[itW].values,intPcpTh)

                # create nan matrix of nb of rec per day
                tmpIntPcpFilt=np.empty(nRecsPerDay)
                tmpIntPcpFilt[:] = np.nan

                #deaccumulate precip and fill it in the new matrix
                tmpIntPcpFilt[jtYaN[1:]] = np.diff(tmpCumPcpFilt[jtYaN])
                tmpIntPcpFilt[jtYaN[0]] = np.nan

                # create an array of zeros of size nRecsPerDay
                tmpIntEvap = np.zeros(nRecsPerDay)

                # fill the nan values identify by jtNaN
                tmpIntEvap[jtNaN] = np.nan

                # We consider that all precip are there. no evap
                # Create an array of ones of size nRecsPerDay
                flagPcp = np.ones(nRecsPerDay)

                #flagEvap=zeros(nRecsPerDay,1);
                # no evap
                flagEvap = np.zeros(nRecsPerDay) # Create an array of zeros of size nRecsPerDay

            # Case the cumul precipitation lower than intPcpTh but greater than negative intPcpTh (-intPcpTh < dPcpW > intPcpTh)
            elif dPcpW>-intPcpTh: #% assumed to be zero

                #There is no precip or evaporation. everything flag to 0
                tmpIntPcpFilt = np.zeros(nRecsPerDay)
                tmpIntPcpFilt[jtNaN] = np.nan

                #There is no precip or evaporation. everything flag to 0
                tmpIntEvap = np.zeros(nRecsPerDay)
                tmpIntEvap[jtNaN] = np.nan

                #There is no precip or evaporation. everything flag to 0
                flagPcp=np.zeros(nRecsPerDay)

                #There is no precip or evaporation. everything flag to 0
                flagEvap = np.zeros(nRecsPerDay)

            # dPcpW < -intPcpTh -- > evaporation
            else: #% evap

                # No precipitation where recorded set precip to 0
                tmpIntPcpFilt=np.zeros(nRecsPerDay)
                tmpIntPcpFilt[jtNaN] = np.nan

                # pass NAF to the opposed evaporation. then reverse it to fit the evap
                tmpCumEvap=-NAF(-cumPcpRaw.iloc[itW].values,intPcpTh)

                #create an empty array to be fill by evap
                tmpIntEvap=np.empty(nRecsPerDay)
                tmpIntEvap[:] = np.nan

                #Fill with the evap value
                tmpIntEvap[jtYaN[1:]] = np.diff(tmpCumEvap[jtYaN])
                tmpIntEvap[jtYaN[0]] = np.nan

                #No precip
                flagPcp = np.zeros(nRecsPerDay)

                #All evap
                flagEvap = np.ones(nRecsPerDay)

            # Evaluate on the different windows
            for iW in range(0, nWindowsPerDay):
              # itArray is the iteration array which will change and move to correspond
              # to the evaluated time.

                #Case of the firt data of the dataset
                # itArray is the same as itW

                if itW1 == 0:
                    itArray=itW
                    jtFilt=itW

                # Case of the last evaluated day of the dataset
                elif itW1 ==  nt-nRecsPerDay:

                    it1 = itW1+ (iW*nRecsPerBlock)

                    it2 = itW1+nWindowsPerDay*nRecsPerBlock

                    itArray=np.arange(it1,it2).astype(int)

                    jtFilt = (itArray-it1).astype(int)

                # All other evaluations
                else:
                    it1=itW1+(iW*nRecsPerBlock) # Displace the index corresponding start to the window
                    it2=itW1+(iW+1)*nRecsPerBlock # Displace the index corresponding end to the window

                    itArray=np.arange(it1,it2).astype(int) # create list of int containing index of evaluated data
                    jtFilt= (itArray-itW1).astype(int) # Make it correspond to the index of the temporary variable

                ## Output from the temporary variable are put in the main variable
                intPcpArray[itArray, iW] = tmpIntPcpFilt[jtFilt]
                intEvapArray[itArray,iW] = tmpIntEvap[jtFilt]
                flagPcpArray[itArray,iW] = flagPcp[jtFilt]
                flagEvapArray[itArray,iW] = flagEvap[jtFilt]

    # Fill regular gaps in Window 1 using Window 2,
    # Window 1 always has extra missing values because
    # the first interval of each 24-h period is always missing.

    if nWindowsPerDay>1:
        #itW1gf=find(isnan(intPcpArray(:,1)) & ~isnan(intPcpArray(:,2)));
        # Find the non np.nan precip value of the Window 2 (~np.isnan(intPcpArray[:,1]) and np.nan
        # precip values of window 1 (np.isnan(intPcpArray[:,0])
        itW1gf = np.where((np.isnan(intPcpArray[:, 0])) &
                          (~np.isnan(intPcpArray[:, 1])))[0]

        # Fill precip for window 1
        intPcpArray[itW1gf, 0] = intPcpArray[itW1gf, 1]

        # Find the non np.nan evap value of the Window 2 (~np.isnan(intPcpArray[:,1]) and np.nan
        #   evap values of window 1 (np.isnan(intPcpArray[:,0])
        itW1gf = np.where(np.isnan(intEvapArray[:, 0]) &
                          (~np.isnan(intEvapArray[:, 1])))

        # Fill evap for window 1
        intEvapArray[itW1gf,0] = intEvapArray[itW1gf,1]

    # There is just 1 window per day. Fill the gap with 0 using gap_size()
    else:
        #ftGF=gap_size(intPcpArray)==1;
        ftGF=gap_size(intPcpArray) == 1
        #intPcpArray(ftGF)=0;
        intPcpArray[ftGF] = 0

        #ftGF=gap_size(intEvapArray)==1;
        ftGF=gap_size(intEvapArray) == 1
        intEvapArray[ftGF] = 0;

    # Use restore_gaps to fill missing data. See the function for details
    intPcpArray,flagPcpArray = restore_gaps(intPcpArray,flagPcpArray,nWindowsPerDay,cumPcpRaw,nt)

    # Average every timestep over all windows for precip
    intPcpFilt = np.nanmean(intPcpArray,axis=1)

    # Sum every timestep over all windows for precip flag
    nFlagPcp = np.nansum(flagPcpArray,axis=1)

    # Average every timestep over all windows for evap
    intEvap = np.nanmean(intEvapArray,axis=1)
    # Sum every timestep over all windows for evap flag
    nFlagEvap = np.nansum(flagEvapArray,axis=1)

    # Evaluate the flags for dependint of the fDetect choice
    if fDetect == 'Any': # Needs one flag per timestep to consider precip or evap
        fPcp = (nFlagPcp>0)
        fEvap = (nFlagEvap>0)
    elif fDetect == 'All': # Needs all windows flags per timestep to consider precip or evap
        fPcp = (nFlagPcp==nWindowsPerDay)
        fEvap = (nFlagEvap==nWindowsPerDay)
    elif fDetect == 'Half':# Needs Half windows flags per timestep to consider precip or evap
        fPcp = (nFlagPcp>=nWindowsPerDay/2)
        fEvap = (nFlagEvap>=nWindowsPerDay/2)

    # Fetch the precip flagged with fDetect
    intPcpFilt[~fPcp] = 0

    # Fetch the evap flagged with fDetect
    intEvap[~fEvap] = 0

    # reacumulated with filtered precipitations
    cumPcpFilt = np.nancumsum(intPcpFilt, axis=0)

    # Naf everything to get rid of any negative artifacts
    cumPcpFilt=NAF(cumPcpFilt,intPcpTh)

    # reacumulated with filtered evaporation
    cumEvap = np.nancumsum(intEvap,axis=0)
    # reverse Naf everything to get ride of any negative artifacts
    cumEvap=-NAF(-cumEvap,intPcpTh)

    return t, cumPcpFilt, cumEvap


def NAF(pRaw,dpTh):
    '''
    function NAF

    Written by Alan Barr, 28 Aug 2012, Environment and Climate Change Canada,
    Climate Research Division, Saskatoon, Canada
    Transpose to Python by Jean-Benoit Madore Apr 2022

    Corresponding author: Craig D. Smith, Environment and Climate Change
    Canada, Climate Research Division, Saskatoon, Canada
    craig.smith2@canada.ca

    The NAF algorithm cleans up a cumulative precipitation time series (Pcp)
    by transferring changes below a specified threshold dpTh
    to neighbouring periods,and eliminating large negative changes
    associated with gauge servicing (bucket emptying).

    Syntax: pNAF=NAF(pRaw,dpTh)

    ************************************************************************
    Inputs: pRaw, dpTh

    pRaw: numpy array: Measured cumulative precipitation time series derived
    from the differential bucket weight and can have a time resolution from
    1-minute to hourly

    dpTh: float: Minimum interval precipitation threshold for the filter.
          Typically set to a value between 0.05 and 0.1, depending on
          instrument precision and uncertainty.
    ************************************************************************
    Outputs: pNAF

    PcpClean: numpy array: Filtered precipitation time series with the same
         temporal resolution as the input time series, pRaw
    ************************************************************************

    The filtering is done using a "brute-force" algorithm (Pan et al., 2015)
    that identifies small or negative changes (below dpTh)then transfers
    them to neighbouring positive changes thus aggregating all changes to
    values above dpTh. The transfers are made in ascending order,
    starting with the lowest (most negative). The cumulative total remains
    unchanged. See Smith et al. (2019) for process description

    Revisions:
    20181204   Amber Ross (ECCC, CRD, Saskatoon),condition added to deal with
               cases where the net accumulation is negative

    References:

    Pan, X., Yang, D., Li, Y., Barr, A., Helgason, W., Hayashi, M., Marsh, P.,
    Pomeroy, J., and Janowicz, R. J.: Bias corrections of precipitation
    measurements across experimental sites in different ecoclimatic regions of
    western Canada, The Cryosphere, 10, 2347-2360,
    https://doi.org/10.5194/tc-10-2347-2016, 2016.

    Smith, C. D., Yang, D., Ross, A., and Barr, A.: The Environment and
    Climate Change Canada solid precipitation intercomparison data from
    Bratt’s Lake and Caribou Creek, Saskatchewan, Earth Syst. Sci.
    Data Discuss., https://doi.org/10.5194/essd-2018-110, in review, 2018.
    '''


    # Base the analysis on non-missing values only
    # by abstracting a sub-time series <xPcp>.

    # Find not nan values
    iYaN=np.where(~np.isnan(pRaw))[0]

    # Select non-nan values within original array
    xPcp=pRaw[iYaN]

    # Base the analysis on interval precip <dxPcp>.
    # Deaccumulate and add 0 at the begining of the array
    dxPcp = np.insert(np.diff(xPcp),0,0)

    # Eliminate servicing drops.
    # All value below -10 are considered either service or bad values
    dpServicingTh=-10
    # Find all servicing values
    itServicing=np.where(dxPcp<dpServicingTh)[0]

    # Delete services from precip array
    dxPcp = np.delete(dxPcp, itServicing)
    # Delete services index in the not nan index array
    iYaN =np.delete(iYaN, itServicing)


    #Dec 4, 2018
    #condition added to deal with cases where the net accumulation is
    #negative

    if sum(dxPcp)>dpTh:# check if data makes sens?

        #Identify small <Drops> to be aggregated to <dpTh> or higher

        # Find all values that are both bellow the precip threshold
        # and not equal to 0
        iDrops = np.where( (dxPcp<dpTh) & (dxPcp!=0) )[0]
        # Count the number of negative values
        nDrops=len(iDrops)

        #Transfer the small <Drops> one at a time,
        #and reassign small <Drops> after each transfer.
        #<Drops> are transferred to <Gets>.

        iPass=0
        while nDrops>0:
            # Count the number of iteration during the while loop
            iPass+=1

            # Find index of the lowest dxPcp to be eliminated
            # Note the subset of dxPCP[iDrops]
            jDrop = np.argmin(dxPcp[iDrops])
            # Get the index within the index array iDrops that is all
            # identified negative values
            iDrop=iDrops[jDrop]

            ############ METHOD FOR DROP TRANSFER ########################
            # Find nearest neighbour <Gets> to transfer <Drop> to.
            # Include in neighbour id criteria not only the distance
            # between points <d2Neighbour> but also the neighbours' <dxPcp>
            # value (with a small weighting) to select higher <dxPcp> if two
            # neighbours are equidistant
            ###############################################################

            # Get all positive values
            iGets=np.where(dxPcp>0)[0]

            # Generate an array of all positive values excluding iDrop
            # (iDrop could be positive if iDrop< dpTh and iDrop > 0)
            iGets=np.setdiff1d(iGets,iDrop)

            # Absolute values of distance index around iDrop
            d2Neighbour=abs(iGets-iDrop) # number of periods apart.

            # Get all values identify as iGets. See above
            dxPcpNeighbour=dxPcp[iGets]
            ### [dN,jGet]=min(d2Neighbour-dxPcpNeighbour/1e5); iGet=iGets(jGet);

            # Find neighbour index with precipition ponderation
            # Find the closest with lesser precip.
            jGet = np.argmin(d2Neighbour-dxPcpNeighbour/1e5)
            # Get index in the iGets array of positive index
            iGet=iGets[jGet]

            # transfer <Drop> to <Get> and set <Drop> to zero.

            dxPcp[iGet] = dxPcp[iGet]+dxPcp[iDrop]

            dxPcp[iDrop] = 0

            # reset <iDrops> and <nDrops>

            iDrops = np.where((dxPcp<dpTh) & (dxPcp!=0))[0]
            nDrops=len(iDrops)

    # Generate empty numpy array to get the new filtered values
    pNAF = np.empty(len(pRaw))
    # put np.nan to all values
    pNAF[:] = np.nan
    # Reacumulate precipitation to not nan iYan indexes
    pNAF[iYaN] = np.nancumsum(dxPcp)

    return pNAF


def gap_size(x):
    '''
    [gs]=gap_size(x)
    gap_size determines the number of contiguous missing data
    for each element of the column vector x.
    Transpose to Python by Jean-Benoit Madore Apr 2022
    '''
    # Make a nan mask based on bolean 1 or 0
    fNaN=np.isnan(x).astype(int)

    # Array of 0 of len(x)
    gs=np.zeros(np.shape(x))

    # Lenght of initial array
    lx=len(x)

    ##### Eval gaps ########
    ### All no data as a value of 1.

    # Diff of 1 means the end of gap
    # Diff of 0 means no change

    # Diff of 1 means the begening of a gap
    # Find all gap start
    # np.diff remove one value. +1 to adust gap start
    iGapStart = np.where(np.diff(fNaN)== 1)[0] + 1

    # Manage case where array x start with a gap
    if fNaN[0]==1:
        iGapStart=np.append(0, iGapStart)

    # Diff of -1 means the end of gap
    # Find all gap end
    # np.diff remove one value. +1 to adust gap start
    iGapEnd = np.where(np.diff(fNaN) == -1)[0] +1

    # Manage case where array x end with a gap
    # Add index of last value
    if fNaN[-1]==1:
        iGapEnd = np.append(iGapEnd, lx-1)

    # number of gaps
    nGaps=len(iGapStart)


    for i in range( 0, nGaps):
        # iterate through the indexes

        if iGapEnd[i] == iGapStart[i]: # Case the gap size is one element
            gs[iGapStart[i]] = 1
        else: # Put the number of item in the gap as value in the gs array
            gs[iGapStart[i]:iGapEnd[i]]=iGapEnd[i]-iGapStart[i]

    return gs


def restore_gaps(intPcpArray,flagPcpArray,nWindowsPerDay,cumPcpRaw,nt):
    '''
    function [intPcpArray,flagPcpArray] =     restore_gaps(intPcpArray,flagPcpArray,nWindowsPerDay,cumPcpRaw,nt)

    restore_gaps - Developed for PcpFiltPosTh24hMovingWindow
    Due to the nature of PcpFiltPosTh24hMovingWindow, precipitation
    is not preserved during all gaps. This is dependent on the number
    of windows that are successful in preserving precipitation during
    gaps. Two cases are examined in which gaps are restored ('All' flag):
      - at least one window preserves pcp during gap
      - no windows preserve pcp during gap

    Syntax: [intPcpArray,flagPcpArray] =
            restore_gaps(intPcpArray,flagPcpArray,nWindowsPerDay,cumPcpRaw,nt)
    Inputs:
    All inputs are from PcpFiltPosTh24hMovingWindow from the NAF-SEG function

    Outputs:
    intPcpArray and flagPcpArray with missed gaps restored
    Written by Amber Ross Jan 31, 2019
    Transpose to Python by Jean-Benoit Madore Apr 2022

    '''

    #preRow evaluate row before current iteration
    preRow = np.full((1, nWindowsPerDay), -1)

    # look at each row and column in intPcpArray
    # Iterate through all the row until lenght of precip (nt)
    for row, curRow in enumerate(intPcpArray):
        # Set preRow on last evaluated row. If row == 0 then preRow == curRow
        if row > 0:
            preRow = intPcpArray[row-1,:]
        # Find precip in current row
        curPosNum = np.where(curRow>0)[0]

        # Find no precip in current row
        curZeroNum = np.where(curRow==0)[0]

        # Find missing value in the last row
        preMissing = np.where(np.isnan(preRow))[0]

        # case where at least one window preserves pcp during gap
        if (len(curPosNum)>=1) & (len(curZeroNum)>=1) & (len(preMissing)==nWindowsPerDay):

            intPcpArray[row,curZeroNum] = np.nan #% so zeros aren't included in nanmean
            flagPcpArray[row,:] = 1          # % so it is flagged as precip
        #% case where no windows preserve pcp during gap
        elif (len(preMissing)==nWindowsPerDay) & (~np.isnan(cumPcpRaw.iloc[row])):

            endGap = cumPcpRaw.iloc[row]
            #% look backwards to find the next real number
            for z in range(0,row):

                tmpVar = cumPcpRaw.iloc[row-z]
                if ~np.isnan(tmpVar):
                    startGap = tmpVar
                    break

            jump = endGap-startGap
            if jump > 0.2 :
                intPcpArray[row,:] = jump #% so the nanmean equals the jump
                flagPcpArray[row,:] = 1   #% so it is flagged as precip

    return intPcpArray, flagPcpArray