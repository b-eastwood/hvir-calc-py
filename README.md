# HVIR Calculation Method (Python)

## Overview
This repository contains source code for the calculation of the Australian Heavy Vehicle Infrastructure Register outputs, being Access (A), Leeway (W) and Ride Quality (R).


## Config specs
The default configuration file is config/settings.config, a config file must be provided to the program
this specifices the types in the csv file, the datetime format, and the default method values


## Commandline options
- f: input  filepath (ignored if stdin provided)
- o: output filepath (ignored if stdout set)
- l: logfile location, will not write logfile if location not specified
- a: a method choose from ['iri','limit','avc']
- r: r method choose from ['iri','hati','vcg]. Will default to vcg if invalid method specified
- w: w method, currently not implented, automatic w method handling instead


## Usage
The calculator class can be imported, and used to calculate methods independtly,
The calculator method requries default values, and takes a survey dictionary as input

## Input:
    - A csv file with header names matching the settings.config structure, an error will be thrown if unidentified columns are detected

## Output:
    - a csv file (via stdout or csv writer) with the same structure as input, plus the added columns
        - a,r,w,hvir,maxev,minev,cat