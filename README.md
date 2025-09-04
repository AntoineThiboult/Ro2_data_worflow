# Ro2_data_worflow
Processing of the data collected on the Romaine watershed.


# Naming conventions
====================
The variable names and their unit are described in the ./Resources/Variable_description_full.xlsx. 
Some variables may have a suffix added. In particular:
    - _gf : this means that this column underwent some gap filling.
    - _qf : quality flag. This system will depend on the method used.
    - _strg : storage term.
    - _corr : indicate that the variable underwent a correction for the energy budget (see in #Fluxes section the # Energy balance correction subsection). 
    - _rf : random forest. Used for gap filling
    - _mds : marginal distribution sampling. See Reichstein et al. “On the separation of net ecosystem exchange into assimilation and ecosystem respiration: review and improved algorithm.” (2005).
These suffixes can be combined. For example, the variable LE_gf_mds_qf refers to the quality of the gap filling of the latent heat flux with the marginal distribution sampling method.


# Radiations
============

# Filtering
-----------
1. Cap downward shortwave solar radiation with theoretical maximum value computed from zenithal angle and solar constants. If exceeded, replaced with maximum theoretical value.
2. Filter out downward short and longwave radiation that are affected by snow on sensor during daytime. Downward short and longwave are discarded if the albedo is greater than 0.85.
3. Set all negative measurements to 0.
4. Recompute albedo and create a 2-day (daytime) rolling mean albedo series. Cap downward shortwave with rolling albedo and upward shortwave to minimize the artifacts du to low angle sun rays in early morning and late evenings
5. Detect spikes in downward and upward longwave radiation, and remove them (if downwards appear contaminated, remove downward and upward as well. And vice versa). 
6. Recompute albedo
7. Recompute net radiation

# Gap filling
-------------
Gap filling is performed only for the stations 'Water stations', 'Forest stations', and 'Bernard Lake'. Gaps in the variables 'rad_longwave_down_CNR4', 'rad_shortwave_down_CNR4', 'rad_longwave_up_CNR4', 'rad_shortwave_up_CNR4' are filled by using ERA5 Land reanalysis that are corrected with in situ measurements and a random forest regressor. Albedo and net radiation are subsequently recomputed with continuous series. 


# Fluxes
========

# Correction of H2O density
Negative H2O density is sometime measured by the Irgason for very low temperature (<-20°C). The source of this error has not been clearly identified: it could indicate a potential H20 density drift with temperature, or a small error made during the calibration in summer. At -25°C, a large variation of relative humidity (RH) results only in a very small change in air water content (40%<RH<80% translates into 0.3<H20 density<0.6g/m3). Therefore, with the HMP45C that measures both temperature and RH, it is possible to see the mean drift of the Irgason. This correction is applied linearly, with zero correction at 20°C (temperature of calibration) and the HMP45C/Irgason difference at -25°C.

# Raft motion correction
This only applies for the "reservoir" station (Romaine-2 reservoir raft). The raft undergoes motion due to waves. This motion will create an apparent wind (measured by the sonic anemometer) that is different from the true wind. The true wind is obtained by correcting the wind measurement with an accelerometer placed next to the sonic (for the equations, see Miller et al., Platform Motion Effects on Measurements of Turbulence and Air–Sea Exchange over the Open Ocean, Journal of Atmospheric and Oceanic Technology, 25(9), 1683-1694, 2008)

# Filtering and correction of the fast data
Filtering performed by EddyPro on the fast 10Hz data (gas densities and sonic wind speed). Flux is computed on the period if at least 60% of the data remains after the different filters. 
1. Instrument diagnostic flags (remove data if instruments raised any malfunction flag)
2. Wind direction filter. 
3. Coordinate rotation (double rotation) 
4. Linear detrending
5. Maximization of the covariance to compensate for time lags between sonic and gas analyzer
6. Spike removal
7. Amplitude resolution
8. Drop-outs
9. Absolute limits of gas concentration
10. Skewness and kurtosis
11. WPL correction
12. Correction of high pass and low pass filtering effects
For more details about points 
    - 1 and 2 see https://www.licor.com/env/support/EddyPro/topics/introduction-dataset-selection.html
    - 3 see https://www.licor.com/env/support/EddyPro/topics/anemometer-tilt-correction.html
    - 4 to 12 see https://www.licor.com/env/support/EddyPro/topics/statistical-tests.html
For the detailed values used in the filters for a given station, please refer to the EddyPro configuration files located in ./Config/EddyProConfig

# Filtering of the slow data
Filtering of the 30-minute energy/gas flux data.
1. Low quality flux (qf=2) according to Mauder and Foken, 2004
2. Band pass filter for each gas flux (see process_micromet/filters.py for the values)
3. Low instrument mean signal quality (RSSI)
4. Remove data when WPL correction cannot be applied
5. Remove data when rain is detected
6. Remove spikes (Papale et al., 2006)
7. Friction velocity threshold for land site (Papale et al 2006), and aquatic sites (Lükő et al. 2020)

# Additional corrections
1. Berge site CO2 fluxes before 2022-06-13. In the old EC150 firmware, CO2 absorption was measured at a high frequency, but air temperature at a lower frequency. Therefore, CO2 densities were calculated with a mix of fast CO2 concentrations and slow air temperature. This created a systematic bias. This artifact has been corrected using the procedure described in Russell et al., Adjustment of CO2 flux measurements due to the bias in the EC150 infrared gas analyzer, Agricultural and Forest Meteorology, Volumes 276–277, 2019
2. Forest site (east and west) before 2022-10-22. The problem was described by Burba et al, Addressing the influence of instrument surface heat exchange on the measurements of CO2 flux from open-path gas analyzers, Global Change Biology Global Change Biology, 2018. In short, during winter time, the Li7500 is heated by its electronics and the sun. This creates an artificial sensible heat flux within the path of the instrument. To correct this, an artificial neural network (ANN) is trained on the Irgason data, then applied this ANN on the period the Li7500 was in use. The residual between the ANN and the Li7500 fluxes were modelled with a simple linear model that is a function of air temperature and an exponential function for the wind. They are applied sequentially (first linear air temperature, then exponential wind speed). Air temperature correction is applied only for temperature below 10°C.

# Merging of the station data
This concerns the 'Forest stations' and 'Water stations'.
1. Forest stations merges measurements from Foret_ouest ('Bernard spruce moss west'), Foret_est ('Bernard spruce moss east'), Foret_sol ('Bernard spruce moss ground').
2. Water stations merges measurements from Berge ('Romaine-2 reservoir shore') and Reservoir ('Romaine-2 reservoir raft'). 
The rule of merging for the fluxes is simple: if a flux from a particular station is best according to Mauder criteron, this flux is kept. In the case both fluxes have the same quality, they are averaged. 

# Gap filling
Two gap filling algorithms are used: a random forest regressor and the marginal distribution sampling (MDS, see Reichstein et al., On the separation of net ecosystem exchange into assimilation and ecosystem respiration: review and improved algorithm, 2005). The gapfilled series are denoted by the suffix _gf, and with _mds for marginal distribution sampling or _rf for the random forest regressor. There is not obvious best technique, they both have their strength and weaknesses. The quality flag system for the MDS works as follows:
NEE present ?                                                 --> Yes     --> Does nothing
Rg, T, VPD, NEE available within |dt|<= 7 days                --> Yes     --> Filling quality A (case 1)
Rg, T, VPD, NEE available within |dt|<= 14 days               --> Yes     --> Filling quality A (case 2)
Rg, NEE available within |dt|<= 7 days                        --> Yes     --> Filling quality A (case 3)
NEE available within |dt|<= 1h                                --> Yes     --> Filling quality A (case 4)
NEE available within |dt|= 1 day & same hour of day           --> Yes     --> Filling quality B (case 5)
Rg, T, VPD, NEE available within |dt|<= 21, 28,..., 140 days  --> Yes     --> Filling quality B if |dt|<=28, else C (case 6)
Rg, NEE available within |dt|<= 14, 21, 28,..., 140 days      --> Yes     --> Filling quality B if |dt|<=28, else C (case 7)
NEE available within |dt|<= 7, 21, 28,...days                 --> Yes     --> Filling quality C (case 8)

# Footprint
The flux footprint is estimated based on the simple parameterisation FFP. See Kljun, N., P. Calanca, M.W. Rotach, H.P. Schmid, 2015: The simple two-dimensional parameterisation for Flux Footprint Predictions FFP. Geosci. Model Dev. 8, 3695-3713, doi:10.5194/gmd-8-3695-2015, for details. See the header of the function FFP_climatology in ./process_micromet/footprint.py for the detailed meaning of the output.

# Energy balance correction
Latent and sensible heat flux as well as their storage terms are corrected according to Mauder, M, Genzel, S, Fu, J, et al. Evaluation of energy balance closure adjustment methods by independent evapotranspiration estimates from lysimeters and hydrological simulations. Hydrological Processes. 2018; 32: 39– 50. https://doi.org/10.1002/hyp.11397. All the variables that are corrected for the energy balance closure are indicated by the suffix _corr. Note that only the 'Forest station' has this correction implemented, as this technique cannot be applied for aquatic eddy covariance stations. 


# Water temperature (thermistors)
=================================

# Filtering 
Remove 6 hours before and 12 hours after data collection
Remove data when the pressure sensor indicates that the rope is tilted (sensors are moved up in the water column).

# Gap filling
Note that water surface temperature is only measured in summer. Water surface temperature is therefore systematically gapfilled in winter. 
Gaps are filled with several technic applied in the following order
1. Water surface temperature is derived from multidimensional linear regression performed on first meter temperatures
2. Water temperature are derived from a linear regression with temperature above and below target
3. Remaining missing data are filled with yearly averaged temperature to which a linear detrending is applied to ensure reconnection with measurements at both ends of the gap.
4. Remaining missing data are filled with linear interpolation

# Romaine-2 reservoir chain
There are two chains, one installed next to the raft (Romaine-2_reservoir_thermistor_chain-1, L=15), the other in the deepest section of the reservoir (Romaine-2_reservoir_thermistor_chain-2, L=70m). Romaine-2_reservoir_thermistor_chain is the average of both chains and is the only gap filled chain on Romaine-2 reservoir. 

# Bernard lake chain
There is no gap filled version of it yet.

# Ice phenology
Freeze-up and melt of the lakes is monitored with time lapse cameras. It is considered frozen when more 50% of the surface is covered by continuous ice (not fragmented). In the case where no direct view of the ice cover is available, MODIS imagery is used instead.


# Other Meteorological Variables
================================

# Filtering
    - Rainfall (Hyquest TB4) : Measurements are retained only if air temperature has remained above freezing during the past 5 days. This avoids contamination caused by ice or snow obstructing the funnel.
    - Total precipitation (Geonor T200b) : Measurements are filtered using the Segmented Neutral Aggregating Filter (NAF_SEG; Smith et al., 2019).
    - Wind speed (RMY 05103 anemometer) : Values are discarded if negative or greater than 30 m/s.

# Gap Filling
Certain variables needed to drive land surface models are also gap filled. The specific variables for each station are listed in:
/Config/GapFillingConfig/[station_name]_slow_data.yml
Gap filling is performed in two steps:
1. Linear interpolation: Gaps are first filled by linear interpolation, up to a maximum window length defined per variable. For example, "air_temp_HC2S3": 6 means gaps of up to 6 time steps (3 hours) are interpolated.
2. Reanalysis-based filling: Remaining gaps are filled with ERA5-Land or ERA5 reanalysis data. These values are bias-corrected using a random forest regressor trained on station observations.


# Merging of data from different stations
=========================================

'Water stations', 'Forest stations', and 'Bernard Lake' are in fact a collection of several "sub-stations".

# Water stations
It is composed of:
    - Berge
    - Reservoir
    - Berge_precip
    - Romaine-2_reservoir_thermistor_chain-1
    - Romaine-2_reservoir_thermistor_chain-2
    
Radiations are handled in a specific way. To obtain a continuous annual time series of net radiation over the water surface, we combined the following data sets: net radiation measured from the raft from June to October, net radiation measured from the shore during periods of reservoir freeze-up, assuming equivalent winter conditions on the shore and on the reservoir (similar snow cover). During the transition periods (late April-early June and late October-December), incoming radiation fluxes were taken from the shore site, the reflected shortwave radiation from the reservoir was based on the albedo calculated from Patel and Rix (2019), and the emitted longwave radiation was estimated from Stefan-Boltzmann’s law considering a surface water temperature estimated from the 0.2-m deep sensor. A water emissivity of 0.99 was used because it provided the best comparison between the raft net radiometer measurements and the empirical Stefan-Boltzmann’s law using the water surface temperature in open water.

# Forest stations
It is composed of:
    - Foret ouest
    - Foret est
    - Foret sol
    - Foret precip
    - Foret neige

# Bernard lake
It is composed of:
    - Bernard lake
    - Bernard_lake_thermistor_chain
    - Foret precip
