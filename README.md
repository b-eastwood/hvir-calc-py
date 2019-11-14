# HVIR Calculation Method (Python)

## Overview
This repository contains source code for the calculation of the Australian Heavy Vehicle Infrastructure Register outputs, being Access (A), Leeway (W) and Ride Quality (R).

## Quickstart 
-  Install python3 via https://www.python.org/downloads/
    - NOTE: MAKE SURE YOU THE SELECT "ADD TO PATH" CHECKBOX BEFORE CLICKING INSTALL NOW
-  Download the repo as a zip file, copy this to your root directory or to your downloads folder
-  Open the commandline/console, you can do this by pressing 
    windows+r, then typing
    cmd.exe 
-  Navigate to the repo folder, or copy the zip file to the C:/ root folder extract the zip, and type the following into the commandline: 


    cd hvir-calc-py
    
    
-  Copy the sample data into the folder, in the same folder as main.py
-  Execute the script by typing the following in to the commandline


    python main.py -o <input_filename.csv> -f outfile.csv -a iri -r hati     
       
## Config specs
The default configuration file is config/settings.config, a config file must be provided to the program
this specifices the types in the csv file, the datetime format, and the default method values

## Commandline options
    - f: input  filepath (ignored if stdin provided)
    - o: output filepath (ignored if stdout set)
    - l: logfile location, will not write logfile if location not specified
    - a: a method choose from ['iri','limit','avc']
    - r: r method choose from ['iri','hati','vcg']. Will default to vcg if invalid method specified
    - w: w method, currently not implented, automatic w method handling instead
    - d: debug, set to 1 to view errors 

## Usage
The calculator class can be imported, and used to calculate methods independtly,
The calculator method requries default values, and takes a survey dictionary as input
Reading and writing via an interactive shell is also possible by importing the python files into a jupyter notebook.

## Input:
    - A csv file with header names matching the settings.config structure, an error will be thrown if unidentified columns are detected

## Output:
    - results csv file (via stdout or csv writer) with the same structure as input, plus the added columns
        - a,r,w,hvir,maxev,minev,cat
    - quality csv file an evaluation of data completeion, accuracy and timeliness, joined to the original output file via the unique id column
        - Master types ac_dat as_dat env_dat con_dat fin_dat inv_dat loc_dat op_dat ref_dat 
        - Each master type has up to 3 columns, accuracy completeness and timeliness 



## Folders: 
- config/
    - This is where the settings for creating the quality and datatype formats are stored. Only developers should edit these files.
    - If you need to change the date format, you can find this setting in the quality.config file
- docs/
    - All documenation regarding the source code can be found here