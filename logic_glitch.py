#!usr/bin/python

import pymssql
import time
import datetime
import itertools
import sys
import matplotlib.pyplot as plt
import numpy as np
from glitch import glitchme as glitchme
from glitch import create_glitched_output as finalize

# drange is global function, super useful
def drange(start, stop, step):
  ''' returns a date range generator '''
  r = start
  while r < stop:
      yield r
      r += step


class Glitcher(object):

    """ generic class for all the glitched things. u is the yaml file that is open """

    def __init__(self, u, name, startdate, enddate, interval):
        self.table = table
        self.name = name
        self.startdate = startdate
        self.enddate = enddate
        self.interval = interval
        _, self.cursor = connect()

    def connect():
        ''' connect to the fsdbdata database'''
        conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
        cursor = conn.cursor()

        return conn, cursor

    def get_data_in_range(selected_probe):
        """ special cases on wind, nr, snc """

        if self.table in ['MS04314','MS00114']:
            # special cases on wind - break into three glitches, then do special finalize
            flag_mag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'MAG' in x][0]
            val_mag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'MAG' in x][0]
            flag_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'SPD' in x][0]
            val_spd_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'SPD' in x][0]
            flag_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'DIR' in x][0]
            val_dir_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'DIR' in x][0]

            query = "select " + u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_mag_word + ", " + flag_mag_word + ", " val_dir_word + ", " + flag_dir_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + u.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + u.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + selected_probe + "\'"

            cursor.execute(query)

            for row in cursor:
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

            query = "select " + u[self.table]['date_word'] +", " + val_spd_word + ", " + flag_spd_word + ", " + val_ux_word + ", " + flag_ux_word + ", " + val_uy_word + ", " + flag_uy_word + "," + val_dir_word + ", " + flag_dir_word + ", " + val_air_word + ", " + flag_air_word +" from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + u.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + u.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + selected_probe + "\'"

            cursor.execute(query)

            for row in cursor:

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
            # special cases on nr
            flag_tot_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x and 'TOT' in x][0]
            val_tot_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x and 'TOT' in x][0]

            query = "select " + u[self.table]['date_word'] +", " + val_tot_word + ", " + flag_tot_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + u.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + u.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + selected_probe + "\'"

            cursor.execute(query)

            for row in cursor:
                dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')
                
                if dt not in valid_data:
                    valid_data[dt] = {'attr': str(row[1]), 'flag': str(row[2])}
                elif dt in valid_data:
                    print("duplicate value at : %s") %(str(row[0]))
                else:
                    pass

            return valid_data

        else:
            # flags and values
            flag_word = [x for x in u[self.table]['mean_words'] if 'FLAG' in x][0]
            val_word = [x for x in u[self.table]['mean_words'] if 'FLAG' not in x][0]

            query = "select " + u[self.table]['date_word'] + ", " + val_word + ", " + flag_word + " from fsdbdata.dbo." + self.table + " where " + u[self.table]['date_word'] + " >= \'" + u.startdate + "\' and " + u[self.table]['date_word'] + " < \'" + u.enddate + "\' and " + u[self.table]['probe_word'] + " like \'" + selected_probe + "\'"

            cursor.execute(query)

            for row in cursor:
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
        results, results_flags = glitchme(my_valid_data, interval)
        final_glitch = finalize(results, results_flags)
        return final_glitch


    def glitchinate_dirs(self, speed, direction):
        theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(speed[each_glitch]['spd_val'], self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd, sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(self.od[probe_code][each_date]['spd_val'],self.od[probe_code][each_date]['dir_val']) if speed != 'None' and x != 'None'])/num_valid_obs_spd)



class PrettyBackground(object):

    """ generic class for plotting Glitchers against one another"""

    def __init__(self)
        import matplotlib.pyplot as plt
        import numpy as np
        self.GlitcherArray = []
        