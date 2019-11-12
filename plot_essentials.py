# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 09:54:13 2019

@author: ANTHI182
"""

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

berge=pd.read_csv("Data_for_tests\Merged_csv\Berge\Merged_data.csv")
berge.index=pd.to_datetime(berge.TIMESTAMP, yearfirst=True)
reservoir=pd.read_csv("Data_for_tests\Merged_csv\Reservoir\Merged_data.csv")
reservoir.index=pd.to_datetime(reservoir.TIMESTAMP, yearfirst=True)
foret_est=pd.read_csv("Data_for_tests\Merged_csv\Foret_est\Merged_data.csv")
foret_est.index=pd.to_datetime(foret_est.TIMESTAMP, yearfirst=True)
foret_ouest=pd.read_csv("Data_for_tests\Merged_csv\Foret_ouest\Merged_data.csv")
foret_ouest.index=pd.to_datetime(foret_ouest.TIMESTAMP, yearfirst=True)


refDate=pd.concat([berge.TIMESTAMP,reservoir.TIMESTAMP,foret_est.TIMESTAMP,foret_ouest.TIMESTAMP],axis=1)

refDate=matplotlib.dates.date2num(refDate.index)
LE=pd.concat([berge.LE, reservoir.LE, foret_est.LE, foret_ouest.LE], axis=1)

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(refDate,LE)
ax.set_ylim(-100,500)

fig.savefig("example.pdf")
#foret_est=pd.read_csv("Data_for_tests\Merged_csv\Foret_est\Merged_data.csv")
#axb=foret_est.LE.plot.line(ylim=(-100,500))
#
#foret_ouest=pd.read_csv("Data_for_tests\Merged_csv\Foret_ouest\Merged_data.csv")
#axb=foret_ouest.LE.plot.line(ylim=(-100,500))


