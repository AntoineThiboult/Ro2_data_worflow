The csidft_convert.exe command line utility is installed by default in the LoggerNet program directory at C:\Program Files (x86)\Campbellsci\LoggerNet\csidft_convert.exe. It takes as input the name of a data file in one of Campbell Scientific’s standard formats and will create a second file in another specified format.
It has the following syntax:
csidft_convert input_file_name output_file_name output-format
where:
input_file_name = the file name of your input file
output_file_name = the output file name to be created
output-format = one of the following options: toaci1, toa5, tob1, csixml, custom-csv, no-header

When converting an array-based file, you must include the following parameters:
--fsl = fsl_file (This is the *.FSL file for the input file.)
--array = array_id (The array id of the array in the input file to be converted.)
All output formats other than toaci1 have an additional optional parameter:
--format-options = format-options (This is an integer value as described below for the different output formats. In each case, add the numbers together for all desired options and input this number in the --format-options parameter.)


Examples
The following example converts myinput.dat to TOA5 format and stores it in myoutput.dat:
csidft_convert.exe myinput.dat myoutput.dat toa5