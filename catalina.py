#!usr/bin/python
# catalina.py

from glitch import *
import csv
import datetime

""" 
A script to run 15 minute ppt for Catalina for all the sites from 10/1/2010 to now. Will produce 15 minute ppt glitched outputs. Will also add in flags. Runs only on current probes. to change interval length, alter input to 'glitchme' function
"""


def connect():
    ''' connect to the fsdbdata database'''
    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor


def get_the_data(cur, probe_code):
    """ gathers the specific data needed by Catalina"""
    
    od = {}

    query = "select date_time, precip_tot, PRECIP_TOT_FLAG from fsdbdata.dbo.ms04313 where date_time > \'2010-10-02 23:55:00\' and date_time <= \'2015-01-01 00:05:00\' and probe_code like \'" + probe_code +"\' order by date_time asc" 

    cur.execute(query)

    for row in cur:
        dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')
        
        try:
            precip_tot = str(row[1])
        except Exception as exc:
            precip_tot = 'None'

        precip_tot_flag = str(row[2])

        if dt not in od:
            od[dt] = {'attr': precip_tot, 'flag': precip_tot_flag}

        elif dt in od:
            print "Date %s is already listed for %s" (str(row[0]), probe_code)
    return od 


def print_high_vals(csvfilename):

    with open(csvfilename,'rb') as readfile:
        reader = csv.reader(readfile)
        reader.next()
        for row in reader:
             if float(row[1]) > 5.0:
                     print str(row[0]) + " is high at " + str(row[1])
             else:
                     continue

if __name__ == "__main__":

    conn, cursor = connect()
    
    probe_code_list = ['PPTCEN02']

    for probe_code in probe_code_list:

        valid_data = get_the_data(cursor, probe_code)
        results = glitchme(valid_data,15, precip=True)
        import pdb; pdb.set_trace()

        fg = create_glitch(results, precip=True)
        

        # name of output file
        built_name = "ppt_glitch3_" + probe_code + ".csv"

        csv_that_glitch(fg, csvfilename = built_name)

        print "finished processing... " + probe_code