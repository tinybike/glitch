#!usr/bin/python
import yaml
import pymssql
import time
import datetime
import itertools
import sys
import matplotlib.pyplot as plt
import numpy as np
from glitch import glitchme as glitchme
from glitch import create_glitch as finalize
from glitch import create_glitched_dirs
from glitch import create_glitched_mags
from glitch import csv_that_glitch
from glitch import csv_that_sonic_glitch
from glitch import csv_that_solar_glitch
from glitch import csv_that_windy_glitch
import math
import re

# drange is global function, super useful
def drange(start, stop, step):
  ''' returns a date range generator '''
  r = start
  while r < stop:
      yield r
      r += step

def connect():
    ''' connect to the fsdbdata database'''
    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor

class Glitcher(object):

    """ generic class for all the glitched things. 
    u is the yaml file that is open 
    table is teh full name of the table, such as ms04315
    name is the probe name, which must be obtained or set
    end date is the last dt to glitch to which must be obtained or set
    start date is the first dt to glitch from, which must be obtained or set
    interval is the number of minutes over which we glitch
    """

    def __init__(self, table, name, startdate, enddate, interval):
        self.table = table
        self.name = name
        self.startdate = startdate
        self.enddate = enddate
        self.interval = interval
        _, self.cursor = connect()
        self.u = self.get_yaml()

    def get_yaml(self):
        """ opens the yaml file which has the mapping of the database tables"""
        strfilename = self.table[0:5] + ".yml"
        u = yaml.load(open(strfilename,'r'))
        return u


    def get_data_in_range(self):
        """ special cases on wind, nr, solar, snc """

        if self.table in ['MS04314', 'MS00114']:
            # speed
            valid_data = {}
            # magnitude
            valid_data2 = {}
            # direction
            valid_data3 = {}

            val_dir_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'DIR' in x][0]
            val_mag_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'MAG' in x][0]
            val_spd_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'SPD' in x][0]

            # flag words are a little different on the old tables!
            if self.table == 'MS00114':
                flag_mag_word = 'FW3'
                flag_spd_word = 'FW1'
                flag_dir_word = 'FW5'
            
            else: 
                # special cases on wind - break into three glitches, then do special finalize
                flag_mag_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'MAG' in x][0]  
                flag_spd_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'SPD' in x][0]
                flag_dir_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'DIR' in x][0]


            query = "select " + self.u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_mag_word + ", " + flag_mag_word + ", " + val_dir_word + ", " + flag_dir_word + " from fsdbdata.dbo." + self.table + " where " + self.u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + self.u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + self.u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                # speed - row 1 is speed
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))

                # mag - second to be called
                if dt not in valid_data2:
                    valid_data2[dt] ={ 'attr': str(row[3]), 'flag': str(row[4])}
                elif dt in valid_data2:
                    print("duplicate value at : %s") %(str(row[0]))
                
                # dir - third to be called
                if dt not in valid_data3:
                    valid_data3[dt] ={ 'attr': str(row[5]), 'flag': str(row[6])}
                elif dt in valid_data3:
                    print("duplicate value at : %s") %(str(row[0]))
                
                else:
                    pass

            # SPEED, MAGNITUDE, DIRECTION
            return valid_data, valid_data2, valid_data3

        elif self.table in ['MS04315', 'MS00115']:
            valid_data = {}
            valid_data2 = {}

            val_mean_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'MEAN' in x][0]
            val_tot_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'TOT' in x][0]
            
            flag_mean_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'MEAN' in x][0]  
            flag_tot_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'TOT' in x][0]

            query = "select " + self.u[self.table]['date_word'] +", " + val_mean_word + ", " + flag_mean_word + ", " + val_tot_word + ", " + flag_tot_word + " from fsdbdata.dbo." + self.table + " where " + self.u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + self.u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + self.u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)
            print query

            for row in self.cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                # mean
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))

                # tot
                if dt not in valid_data2:
                    valid_data2[dt] ={ 'attr': str(row[3]), 'flag': str(row[4])}
                elif dt in valid_data2:
                    print("duplicate value at : %s") %(str(row[0]))
            
            # mean, tot
            return valid_data, valid_data2


        elif self.table in ['MS04334', 'MS00134']:
            # speed
            valid_data = {}
            # ux
            valid_data2 = {}
            # uy
            valid_data3 = {}
            # dir
            valid_data4 = {}
            # airt
            valid_data5 = {}

            # special cases on snc
            flag_ux_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'UX' in x][0]
            val_ux_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'UX' in x][0]
            flag_spd_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'SPD' in x][0]
            val_spd_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'SPD' in x][0]
            flag_dir_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'DIR' in x][0]
            val_dir_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'DIR' in x][0]
            flag_uy_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'UY' in x][0]
            val_uy_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'UY' in x][0]
            flag_air_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'AIR' in x][0]
            val_air_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'AIR' in x][0]

            query = "select " + self.u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_ux_word + ", " + flag_ux_word + ", " + val_uy_word + ", " + flag_uy_word + "," + val_dir_word + ", " + flag_dir_word + ", " + val_air_word + ", " + flag_air_word +" from fsdbdata.dbo." + self.table + " where " + self.u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + self.u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + self.u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:
                print row

                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                # spd
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))
                
                # ux
                if dt not in valid_data2:
                    valid_data2[dt] = {'attr': str(row[3]), 'flag': str(row[4])}
                elif dt in valid_data2:
                    print("duplicate value at : %s") %(str(row[0]))

                # uy
                if dt not in valid_data3: 
                    valid_data3[dt] = {'attr': str(row[5]), 'flag': str(row[6])}
                elif dt in valid_data3:
                    print("duplicate value at : %s") %(str(row[0]))

                # dir
                if dt not in valid_data4: 
                    valid_data4[dt] = {'attr': str(row[7]), 'flag': str(row[8])}
                elif dt in valid_data4:
                    print("duplicate value at : %s") %(str(row[0]))

                # air
                if dt not in valid_data5:
                    valid_data5[dt] = {'attr': str(row[9]), 'flag': str(row[10])}
                elif dt in valid_data5:
                    print("duplicate value at : %s") %(str(row[0]))
                
                else:
                    pass

            # spd, ux, uy, dir, air
            return valid_data, valid_data2, valid_data3, valid_data4, valid_data5

        elif self.table in ['MS04335', 'MS00135']:
            valid_data = {}
            # special cases on nr
            flag_tot_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x and 'TOT' in x][0]
            val_tot_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x and 'TOT' in x][0]

            query = "select " + self.u[self.table]['date_word'] +", " + val_tot_word + ", " + flag_tot_word + " from fsdbdata.dbo." + self.table + " where " + self.u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + self.u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + self.u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')
                
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))
                else:
                    pass

            return valid_data

        else:

            valid_data = {}
            
            # flags and values
            if self.table not in ['HT00411','MS00511','HT00434','MS00512','MS00531']:
                
                #print "KEY WORD is listed; TABLE IS NOT a weird one in HT004 or MS005"
                
                val_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' not in x][0]
                flag_word = [x for x in self.u[self.table]['mean_words'] if 'FLAG' in x][0]
            
            # elif self.table[0:5] == 'MS001':
            #     # flags in MS001 have very different names
            #     lookup = {'MS00111': 'FT1',
            #                 'MS00112': 'FR1',
            #                 'MS00113': 'FP1',
            #                 'MS00115': 'FS1',
            #                 'MS00116': 'FM1',
            #                 'MS00117': 'FD1',
            #                 'MS00118': 'FV1',
            #                 'MS00119': 'FL1',
            #                 'MS00131': 'FST1',
            #                 'MS00132': 'FPR1'}
                
            #     flag_word = lookup[self.table]
            
            # the HT series
            elif self.table in ['HT00411', 'MS00511']:
                flag_word = 'FT1'
            elif self.table == 'HT0034':
                flag_word = 'FWT1'
            elif self.table == 'MS00512':
                flag_word = 'FR1'
            elif self.table == 'MS00531':
                flag_word = 'FST1'
            else:
                pass

            query = "select " + self.u[self.table]['date_word'] + ", " + val_word + ", " + flag_word + " from fsdbdata.dbo." + self.table + " where " + self.u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + self.u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + self.u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))
                else:
                    pass

            return valid_data

    def glitchinate(self, my_valid_data):
        """ compute the glitches except wind and solar and sonic"""

        if self.table not in ['MS00113','MS04313']:
            results = glitchme(my_valid_data, self.interval, precip=False)
            final_glitch = finalize(results)
        else:
            # if precip is true then the "TOT" record will also be used
            results = glitchme(my_valid_data, self.interval, precip=True)
            final_glitch = finalize(results, precip=True)

        # mean or tot
        return final_glitch

    def glitchinate_sol(self, my_valid_data, my_valid_data2):
        """ compute the glitches except wind-- even solar glitch is ok"""

        results = glitchme(my_valid_data, self.interval, precip=False)
        results2 = glitchme(my_valid_data2, self.interval, precip=True)

        final_glitch1 = finalize(results)
        final_glitch2 = finalize(results2, precip=True)

        # mean, tot
        return final_glitch1, final_glitch2

    def glitchinate_wind(self, my_valid_data_spd, my_valid_data_dir, my_valid_data_mag):
        
        # speed
        results_spd = glitchme(my_valid_data_spd, self.interval)
        # dir
        results_dir = glitchme(my_valid_data_dir, self.interval)
        # mag
        results_mag= glitchme(my_valid_data_mag, self.interval)

        final_glitch_mag = create_glitched_mags(results_spd, results_dir)
        final_glitch_dir = create_glitched_dirs(results_spd, results_dir)
        final_glitch_spd = finalize(results_spd)

        return final_glitch_spd, final_glitch_dir, final_glitch_mag

    def glitchinate_sonic(self, my_valid_data_spd, my_valid_data_ux, my_valid_data_uy, my_valid_data_dir, my_valid_data_air):
        
        # speed
        results_spd= glitchme(my_valid_data_spd, self.interval)
        # dir
        results_dir = glitchme(my_valid_data_dir, self.interval)
        # ux
        results_ux = glitchme(my_valid_data_ux, self.interval)
        # uy
        results_uy = glitchme(my_valid_data_uy, self.interval)
        # air
        results_air= glitchme(my_valid_data_air, self.interval)

        final_glitch_spd = finalize(results_spd, results_dir)
        final_glitch_dir = create_glitched_dirs(results_spd, results_dir)
        final_glitch_ux = finalize(results_ux)
        final_glitch_uy = finalize(results_uy)
        final_glitch_air = finalize(results_air)

        return final_glitch_spd, final_glitch_dir, final_glitch_ux, final_glitch_uy, final_glitch_air

    def decide(self):
        """ decide determines if an input is wind, regular, solar, or sonic"""

        if self.table in ['MS04314', 'MS00114']:
            
            # speed, magnitude, direction are the inputs vd1, vd2, vd3
            try:
                vd1, vd2, vd3 = self.get_data_in_range()
                # speed, direction, magnitude are the inputs to glitchinate wind, fg1, fg2, fg3 are the outputs
                # of speed, direction, magnitude
                fg1, fg2, fg3 = self.glitchinate_wind(vd1, vd3, vd2)

            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg1 = [], fg2 = [], fg3 = []
            
            return fg1, fg2, fg3

        elif self.table in ['MS04334']:
            
            # speed, ux, uy, dir, air are vd1, vd2, vd3, vd4, vd5
            try:
                vd1, vd2, vd3, vd4, vd5 = self.get_data_in_range()
                # speed, dir, ux, uy, air are fg1, fg2, fg3, fg4, fg5, glitchinate takes speed, ux, uy, dir, air
                fg1, fg2, fg3, fg4, fg5 = self.glitchinate_sonic(vd1, vd2, vd3, vd4, vd5)
            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg1 = [], fg2 = [], fg3= [], fg4 = [], fg5 = []
            
            return fg1, fg2, fg3, fg4, fg5
        
        elif self.table in ['MS04315', 'MS00115']:

            try:
                # mean, tot are vd1, vd2
                vd1, vd2 = self.get_data_in_range()
                # mean and total 
                fg1, fg2 = self.glitchinate_sol(vd1, vd2)

            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg1 = [], fg2 = []
            return fg1, fg2


        else:
            # u1 = self.get_yaml()
            try:
                vd1 = self.get_data_in_range()
                fg = self.glitchinate(vd1)
            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg = []

            return fg


class SmartGlitcher(Glitcher):
    """SmartGlitcher methods are used for smart aggregation, including with methods for the wind. """

    def __init__(self, table, name, startdate, enddate, interval):

        super(SmartGlitcher, self).__init__(table, name, startdate, enddate, interval)

        #### FOX YOU ARE HERE MAKING A CSV METHOD FOR THE SOLAR!!! NEARLY THERE!!!

    def tocsv(self,csvfilename):
        import csv        

        # most tables not wind
        if self.table not in ['MS04314', 'MS00114', 'MS04334','MS04315','MS00115']:
            final_glitch = self.decide()
            web_csv = csv_that_glitch(final_glitch, csvfilename)

        # wind
        elif self.table in ['MS04314', 'MS00114']:
            final_glitch1, final_glitch2, final_glitch3 = self.decide()
            web_csv = csv_that_windy_glitch(final_glitch1, final_glitch2, final_glitch3, csvfilename)
        
        # sonic
        elif self.table == 'MS04334':
            final_glitch1, final_glitch2, final_glitch3, final_glitch4, final_glitch5= self.decide()
            web_csv = csv_that_sonic_glitch(final_glitch1, final_glitch2, final_glitch3, final_glitch4, final_glitch5, csvfilename)

        elif self.table in ['MS04315','MS00115']:
            final_glitch1, final_glitch2 = self.decide()
            web_csv = csv_that_solar_glitch(final_glitch1, final_glitch2, csvfilename)

        return web_csv

    # def tonarra(self):
    #     """ single attribute glitches can be written to narra"""
    #     from glitch import html_that_glitch
    #     final_glitch = self.decide()
    #     html_that_glitch(final_glitch)

    def graphme(self, pngfilename="my_sample_png.png"):

        import numpy as np
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import mpld3
        import datetime

        """ creating background info"""
        # create a plot with as may subplots as you choose
        fig, ax = plt.subplots()
        # add a grid to the background
        ax.grid(True, alpha = 0.2)
        # the x axis contains date
        fig.autofmt_xdate()
        # the dates are year, month
        ax.fmt_xdata = mdates.DateFormatter('%Y-%m')

        if self.table not in ['MS04314', 'MS00114', 'MS04334','MS04315','MS00115']:
            final_glitch = self.decide()

            dates = sorted(final_glitch.keys())
            dates2 = [x for x in dates if final_glitch[x]['mean'] != None and final_glitch[x]['mean'] != "None"]
            vals = [final_glitch[x]['mean'] for x in dates2]
            glitched_values = ax.plot(dates2, vals, 'b-')
            ax.legend(loc=4)
            ax.set_xlabel("dates")
            ax.set_ylabel("values")
            mpld3.show()
            mpld3.save_html(fig, 'my_output_html.html')
            import pylab
            pylab.savefig(pngfilename)
