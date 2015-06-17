#!usr/bin/python

import pymssql
import time
import datetime
import itertools
import sys
import matplotlib.pyplot as plt
import numpy as np

class Glitcher(object):

    """ generic class for all the glitched things """

    def __init__(self, startdate, enddate):
        self.name = name
        self.startdate = startdate
        self.enddate = enddate

    def connect():
        ''' connect to the fsdbdata database'''
        conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
        cursor = conn.cursor()

        return conn, cursor


class PrettyBackground(object):

    """ generic class for plotting Glitchers against one another"""

    def __init__(self)
        import matplotlib.pyplot as plt
        import numpy as np
        self.GlitcherArray = []
        