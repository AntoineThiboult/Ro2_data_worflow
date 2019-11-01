import os
import convertRaw2ascii as cr2a

station="Berge"
rawFileDir      =os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Raw_data")
asciiFileDir=os.path.join("C:\\","Users","anthi182","Desktop","Data_for_automatization","Ascii_data")



cr2a.printnames(station,rawFileDir,asciiFileDir)