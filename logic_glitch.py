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
from glitch import create_glitched_output as finalize
from glitch import create_glitched_dirs
from glitch import create_glitched_speeds
from glitch import csv_that_glitch
from glitch import csv_that_sonic_glitch
from glitch import csv_that_windy_glitch
import math

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

    """ generic class for all the glitched things. u is the yaml file that is open """

    def __init__(self, table, name, startdate, enddate, interval):
        self.table = table
        self.name = name
        self.startdate = startdate
        self.enddate = enddate
        self.interval = interval
        _, self.cursor = connect()

    def get_yaml(self):
        strfilename = self.table[0:5] + ".yml"
        u = yaml.load(open(strfilename,'r'))
        return u


    def get_data_in_range(self, u):
        """ special cases on wind, nr, snc """

        if self.table in ['MS04314','MS00114']:
            valid_data = {}
            valid_data2 = {}
            valid_data3 = {}

            # special cases on wind - break into three glitches, then do special finalize
            flag_mag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'MAG' in x][0]
            val_mag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'MAG' in x][0]
            flag_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'SPD' in x][0]
            val_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'SPD' in x][0]
            flag_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'DIR' in x][0]
            val_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'DIR' in x][0]

            query = "select " + u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_mag_word + ", " + flag_mag_word + ", " + val_dir_word + ", " + flag_dir_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                # mag
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))

                # spd
                if dt not in valid_data2:
                    valid_data2[dt] ={ 'attr': str(row[3]), 'flag': str(row[4])}
                elif dt in valid_data2:
                    print("duplicate value at : %s") %(str(row[0]))
                
                # dir
                if dt not in valid_data3:
                    valid_data3[dt] ={ 'attr': str(row[5]), 'flag': str(row[6])}
                elif dt in valid_data3:
                    print("duplicate value at : %s") %(str(row[0]))
                
                else:
                    pass

            return valid_data, valid_data2, valid_data3

        elif self.table in ['MS04334', 'MS00134']:
            valid_data = {}
            valid_data2 = {}
            valid_data3 = {}
            valid_data4 = {}
            valid_data5 = {}
            # special cases on snc
            flag_ux_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'UX' in x][0]
            val_ux_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'UX' in x][0]
            flag_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'SPD' in x][0]
            val_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'SPD' in x][0]
            flag_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'DIR' in x][0]
            val_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'DIR' in x][0]
            flag_uy_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'UY' in x][0]
            val_uy_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'UY' in x][0]
            flag_air_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'AIR' in x][0]
            val_air_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'AIR' in x][0]

            query = "select " + u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_ux_word + ", " + flag_ux_word + ", " + val_uy_word + ", " + flag_uy_word + "," + val_dir_word + ", " + flag_dir_word + ", " + val_air_word + ", " + flag_air_word +" from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + self.name + "\'"

            self.cursor.execute(query)

            for row in self.cursor:

                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

                # ux
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))
                
                # spd
                if dt not in valid_data2:
                    valid_data2[dt] = {'attr': str(row[3]), 'flag': str(row[4])}
                elif dt in valid_data2:
                    print("duplicate value at : %s") %(str(row[0]))

                # dir
                if dt not in valid_data3: 
                    valid_data3[dt] = {'attr': str(row[5]), 'flag': str(row[6])}
                elif dt in valid_data3:
                    print("duplicate value at : %s") %(str(row[0]))

                # uy
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

            return valid_data, valid_data2, valid_data3, valid_data4, valid_data5

        elif self.table in ['MS04335', 'MS00135']:
            valid_data = {}
            # special cases on nr
            flag_tot_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'TOT' in x][0]
            val_tot_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'TOT' in x][0]

            query = "select " + u[self.table]['date_word'] +", " + val_tot_word + ", " + flag_tot_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + self.name + "\'"

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
            flag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x][0]
            val_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x][0]

            query = "select " + u[self.table]['date_word'] + ", " + val_word + ", " + flag_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + self.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + self.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + self.name + "\'"

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
        """ compute the glitches except wind-- even solar glitch is ok"""
        results, results_flags = glitchme(my_valid_data, self.interval)
        final_glitch = finalize(results, results_flags)
        return final_glitch

    def glitchinate_wind(self, my_valid_data_spd, my_valid_data_dir, my_valid_data_mag):
        
        # speed
        results_spd, results_flags_spd = glitchme(my_valid_data_spd, self.interval)
        # dir
        results_dir, results_flags_dir = glitchme(my_valid_data_dir, self.interval)
        # mag
        results_mag, results_flags_mag = glitchme(my_valid_data_mag, self.interval)

        final_glitch_spd = create_glitched_speeds(results_spd, results_dir, results_flags_spd)
        final_glitch_dir = create_glitched_dirs(results_spd, results_dir, results_flags_dir)
        final_glitch_mag = finalize(results_mag, results_flags_mag)

        return final_glitch_spd, final_glitch_dir, final_glitch_mag

    def glitchinate_sonic(self, my_valid_data_ux, my_valid_data_spd, my_valid_data_dir, my_valid_data_uy, my_valid_data_air):
        # speed
        results_spd, results_flags_spd = glitchme(my_valid_data_spd, self.interval)
        # dir
        results_dir, results_flags_dir = glitchme(my_valid_data_dir, self.interval)
        # ux
        results_ux, results_flags_ux = glitchme(my_valid_data_ux, self.interval)
        # uy
        results_uy, results_flags_uy = glitchme(my_valid_data_uy, self.interval)
        # air
        results_air, results_flags_air = glitchme(my_valid_data_air, self.interval)

        final_glitch_spd = create_glitched_speeds(results_spd, results_dir, results_flags_spd)
        final_glitch_dir = create_glitched_dirs(results_spd, results_dir, results_flags_dir)
        final_glitch_ux = finalize(results_ux, results_flags_ux)
        final_glitch_uy = finalize(results_uy, results_flags_uy)
        final_glitch_air = finalize(results_air, results_flags_air)

        return final_glitch_spd, final_glitch_dir, final_glitch_ux, final_glitch_uy, final_glitch_air

    def decide(self):

        if self.table in ['MS04314', 'MS00114']:
            u1 = self.get_yaml()
            try:
                vd1, vd2, vd3 = self.get_data_in_range(u1)
                fg1, fg2, fg3 = self.glitchinate_wind(vd1, vd2, vd3)

            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg1 = [], fg2 = [], fg3 = []
            return fg1, fg2, fg3

        elif self.table in ['MS04334']:
            u1 = self.get_yaml()
            try:
                vd1, vd2, vd3, vd4, vd5 = self.get_data_in_range(u1)
                fg1, fg2, fg3, fg4, fg5 = self.glitchinate_sonic(vd1, vd2, vd3, vd4, vd5)
            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg1 = [], fg2 = [], fg3= [], fg4 = [], fg5 = []
            return fg1, fg2, fg3, fg4, fg5
        else:
            u1 = self.get_yaml()
            try:
                vd1 = self.get_data_in_range(u1)
                fg = self.glitchinate(vd1)
            except IndexError:
                return "<html><body><b> Error: data not found in range </b></body></html>"
                fg = []

            return fg


class SmartGlitcher(Glitcher):
    """SmartGlitcher methods are used for smart aggregation, including with methods for the wind. """

    def __init__(self, table, name, startdate, enddate, interval):

        super(SmartGlitcher, self).__init__(table, name, startdate, enddate, interval)

    # def glitchinate_smart(self):
    #     final_glitch =  self.decide()
    #     return final_glitch

    def tocsv(self,csvfilename):
        import csv

        

        if self.table not in ['MS04314', 'MS00114', 'MS04334']:
            final_glitch = self.decide()
            web_csv = csv_that_glitch(final_glitch, csvfilename)

        elif self.table in ['MS04314', 'MS00114']:
            final_glitch1, final_glitch2, final_glitch3 = self.decide()
            web_csv = csv_that_windy_glitch(final_glitch1, final_glitch2, final_glitch3, csvfilename)
            
        elif self.table == 'MS04334':
            final_glitch1, final_glitch2, final_glitch3, final_glitch4, final_glitch5= self.decide()
            web_csv = csv_that_sonic_glitch(final_glitch1, final_glitch2, final_glitch3, final_glitch4, final_glitch5, csvfilename)

        return web_csv
            


# class PrettyBackground(object):

#     """ generic class for plotting Glitchers against one another"""

#     def __init__(self)
#         import matplotlib.pyplot as plt
#         import numpy as np
#         self.GlitcherArray = []
        