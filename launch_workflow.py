import os
import convertRaw2ascii as cr2a

rawFileDir      =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Raw_data")
asciiOutDir    =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Ascii_data")
eddyproOutDir   =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Eddypro_data")



cr2a.printnames(rawFileDir,asciiFileDir)