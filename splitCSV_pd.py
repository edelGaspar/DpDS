# This script splits giant CSV files into smaller files
# and also removes duplicate rows and rows that meet a criterion (number of events in one minute).
# Params:
#  --datafile      Ex: --datafile' 'D:\EstudioDeAgua\Data\inputs_201805.csv\inputs_201805.csv'
#  -header         Ex: -header false. default its True
#  -nlines         Ex: -nlines 1000. Default its 1000000
#  -nfiles         Ex: -nfiles 30. Default its 30
#  -outputPath     Ex: -outputPath 'D:\EstudioDeAgua\Data\inputs_201805.csv'
#  -measPerMinute  Ex: -measPerMinute 2. Default its 2

# Developed by: edel.gaspar@gmail.com

import datetime
import sys
import argparse
import os
import os.path
import subprocess
from importlib import import_module
import fileinput
import re
import shutil
from tempfile import mkstemp
from os import fdopen, remove
from typing import Any
import pandas as pd
import numpy as np
import time

from pandas.core.frame import DataFrame

def get_arguments():
    description="""This script splits large CSV files and splits them into
     'nfiles' files of 'nlines' number of lines.."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--datafile", required=True)
    parser.add_argument("-header", required=False, default=True)
    parser.add_argument("-nlines", required=False, default='1000000')
    parser.add_argument("-nfiles", required=False, default='30')
    parser.add_argument("-outputPath", required=False, default='.')
    parser.add_argument("-measPerMinute", required=False, default='2')
    return parser.parse_args()

def create_splits(datafile, number_lines, nlines, nfiles, includeHeader, header, outputPath, separator, measPerMinute, column_name):
    count = 0    
    for i in range(0,number_lines,nlines):
        start_time_file = time.time()
        if count == int(nfiles):
            return
        df = DataFrame()
        out_csv = outputPath + separator + 'input' + str(count) + '.csv'

        if includeHeader:       
            df = pd.read_csv(datafile, header=0, nrows = nlines, skiprows = i,  parse_dates=[3])            
            df.columns = header
        else:
            df = pd.read_csv(datafile, header=None, nrows = nlines, skiprows = i, parse_dates=[3])

        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df = filter_dates(df, column_name, 60/measPerMinute)
        df_to_csv(df, out_csv, nlines, False, includeHeader)
        end_time_file = time.time()        
        count += 1
        print(' --> File ' + str(count) + ' generated with ' + str(len(df)) +' lines. In '+ str(end_time_file-start_time_file) + ' seconds')
    print('Done. ' + str(count) + ' files created')

def df_to_csv(df, out_csv, nlines, includeIndex, includeHeader ):
    df.to_csv(out_csv,
            index=includeIndex,
            header=includeHeader,
            mode='w',
            chunksize=nlines)

def filter_dates(df, column_name, seg):
    sec_cero= time.strptime('01-01-1980 00:00:00', '%d-%m-%Y %H:%M:%S')
    sec_thirty= time.strptime('01-01-1980 00:00:30', '%d-%m-%Y %H:%M:%S')
    df=df[(df[column_name].dt.second == sec_cero.tm_sec) | (df[column_name].dt.second == sec_thirty.tm_sec)]
    df = df.drop_duplicates(subset=[column_name])
    return df
        
def main():
       
    if sys.platform == "win32":
        separator = '\\'
    else:
        separator = '/'
    
    args = get_arguments()

    datafile = args.datafile
    if datafile:
        if os.path.isfile(datafile):
            print ("Read config " + datafile)
        else:
            print ("File " + datafile + " not exist")
            return 0

    outputPath = args.outputPath
    if outputPath and outputPath != '.':
        if not os.path.isdir(outputPath):
            return 0

    start_time = time.time()
    number_lines = sum(1 for row in (open(datafile)))
    print('Total lines: ', number_lines)
    nlines = int(args.nlines)
    nfiles = int(args.nfiles)
    measPerMinute = int(args.measPerMinute)

    header = pd.read_csv(datafile, header=0, nrows = 5)
    print(header.head())

    includeHeader = args.header.lower() == 'true'
    
    if includeHeader:
        header = pd.read_csv(datafile, header=0, nrows = 0).to_dict().keys()
    else:
        header = []    

    column_name = 'Fecha'    

    create_splits(datafile, number_lines, nlines, nfiles, includeHeader, header, outputPath, separator, measPerMinute, column_name)
    end_time = time.time()
    print('Duration: ', str(end_time - start_time), 'seconds')   

    
   
if __name__ == '__main__':
    main()