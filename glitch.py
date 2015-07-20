#!usr/bin/python
import csv
import pymssql
import time
import datetime
import itertools
import sys
import math

def describe_me():
  print """
  glitch is the main engine behind our time series aggregation.
  connect - makes connection to fsdb
  drange - generates date ranges
  glitchme - performs glitch aggregation when possible
  createglitchedspeed - performs glitch aggregation on speed of wind
  """

def connect():
    ''' connect to the fsdbdata database'''

    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor


def drange(start, stop, step):
  ''' returns a date range generator '''
  r = start
  while r < stop:
      yield r
      r += step


def glitchme(valid_data, interval, precip=False):
  ''' perform glitch aggregations LIKE A BOSS'''

  # holds the resulting data for each stop point, persists
  results = {}

  # Get all dates where we have valid data to key from
  dr = [x for x in sorted(valid_data.keys())]

  first_date = dr[0] + datetime.timedelta(minutes=interval)
  last_date = dr[-1] + datetime.timedelta(minutes=interval)

  # super_range is an ideal range from each interval to the next one (i.e. 15 minute intervals, 35 minute intervals, etc.)
  super_range = drange(first_date, last_date, datetime.timedelta(minutes=interval))

  # min range is an ideal range for one minute intervals to aggregate
  minrange = [x for x in drange(dr[0], dr[-1], datetime.timedelta(minutes=1))]

  # t_mean stores values for that interval and f_mean stores flags for that interval
  t_mean =[]
  f_mean =[]
  
  # make an iterator to walk the known date range (i.e. 10:00, 10:05, 10:10 etc.)
  starting = iter(dr)

  # ex. 2010-10-01 00:00:00
  this_date = starting.next()

  # ex. 2010-10-01 00:05:00
  subsequent = starting.next()

  # first point to put in for the glitch (ex 2010-10-01 00:15:00)
  checkpoint = super_range.next()

  # the the attribute is precip or solar tot, we need to get a sum, so the one minute values are actually representative of 1/length of interval that quantity

  if precip==True:
    duration_1= (subsequent-this_date)
    duration = duration_1.seconds/60 + duration_1.days*(86400/60)
    
  else:
    duration = 1


  for each_minute in minrange:
    # if the minute is the checkpoint, update the dictionary to the accumulated values
    # for example if it is 10-01-01 13:00:00 and that's a 13 min checkpoint, we get the values from 0-12 minutes in the dictionary for that checkpoint(so it is the previous minutes to that one)

    if each_minute == checkpoint:

      if checkpoint not in results:
        results[checkpoint] = {'val': t_mean, 'fval': f_mean}

      else:
        print("Check point at " + datetime.datetime.strftime(each_minute, '%Y-%m-%d %H:%M:%S') + "is in the rsults already!")
        import pdb; pdb.set_trace()

      # set the storage to empty
      t_mean = []
      f_mean = []

      # update the checkpoint by 1:
      checkpoint = super_range.next()

    else:
      pass


    # we still have to check if the value changes and start accumulating to the next date...
    # this would get say between 10-01-01 00:00:00 and 10-01-01 00:00:04
    if each_minute >= this_date and each_minute < subsequent:

      try:
        t_mean.append(round(float(valid_data[this_date]['attr'])/duration,3))
      except Exception:
        #import pdb; pdb.set_trace()
        print "none value found as " + str(valid_data[this_date]['attr']) + " on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')
        t_mean.append(valid_data[this_date]['attr'])

      f_mean.append(valid_data[this_date]['flag'])

    elif each_minute == subsequent:

      # move up by one in the original search
      this_date = subsequent
      subsequent = starting.next()

      # compute the number of minutes in the duration
      if precip==True:
        duration_1= (subsequent-this_date)
        duration = duration_1.seconds/60 + duration_1.days*(86400/60)
        
      else:
        duration = 1

      try:
        t_mean.append(round(float(valid_data[this_date]['attr'])/duration,3))
      except Exception:
        t_mean.append(valid_data[this_date]['attr'])

      f_mean.append(valid_data[this_date]['flag'])

    elif each_minute >= subsequent:
      print "the minute should not exceed the subsequent"

  return results

def create_glitch(results, precip=False):
  """
  Applies to all criteria EXCEPT Wind Magnitude, Wind Direction, and some of the sonic things
  If the attribute is wind speed, an additional flag is needed, copy from wind mag, where it is implemented
  """
  # output structure
  final_glitch = {}
  for each_glitch in sorted(results.keys()):
    
    if results[each_glitch] != []:

      # if the data is precip (or solar total), the results are a sum of the intermediate values, after you have divided by the duration of the interval. If Nones exist it won't add so you will need to instead do the sum of the intermediate values which are not None.
      if precip==True:

        try:
          meanval = round(sum(results[each_glitch]['val']),2)
        except Exception:
          values = [results[each_glitch]['val'][index] for index,x in enumerate(results[each_glitch]['val']) if x != None and x != "None"]

          if values == []:
            meanval = "None"
          
          else:
            meanval = round(sum(values),2)
      
      # in the generic case the mean is the sum of the results divided by the total number of values contributing to it. But if there are nones, we will need to only divide by the number of relevant values
      else:
        try: 
          meanval = round(sum(results[each_glitch]['val'])/len(results[each_glitch]['val']),2)
        except Exception:
          values = [results[each_glitch]['val'][index] for index,x in enumerate(results[each_glitch]['val']) if x != None and x != "None"]

          if values == []:
            print "The entire interval contains only bad values: " + datetime.datetime.strftime(each_glitch,'%Y-%m-%d %H:%M:%S')
            meanval = "None"
          
          else:
            meanval = round(sum(values)/len(values),2)

      try:
        
        num_flags = len(results[each_glitch]['fval'])

        if 'E' not in results[each_glitch]['fval'] and 'M' not in results[each_glitch]['fval'] and 'Q' not in results[each_glitch]['fval']:
          flagged_val = 'A'
        
        else:
          numM = len([x for x in results[each_glitch]['fval'] if x == 'M'])
          numE = len([x for x in results[each_glitch]['fval'] if x == 'E'])
          numQ = len([x for x in results[each_glitch]['fval'] if x == 'Q'])

          if numM/num_flags > 0.8:
            flagged_val = 'M'
          elif numE/num_flags > 0.05:
            flagged_val = 'E'
          elif (numE + numM + numQ)/num_flags > 0.05:
            flagged_val = 'Q'
          else:
            flagged_val = 'A'
      
      except Exception:
        flagged_val = 'M'

    elif results[each_glitch]['fval'] == []:
      meanval = None
      flagged_val = 'M'

    final_glitch[each_glitch] = {'mean': meanval, 'flags': flagged_val}

  return final_glitch

def create_glitched_mags(results1, results2):
  """
  from the speed we get the x and y components for each direction for the magnitude
  then we add these up over the course of the glitch and take the sqrt
  """
  final_glitch = {}

  for each_glitch in sorted(results1.keys()):
    if results1[each_glitch]['val'] != []:

      num_valid_obs = len(results1[each_glitch]['val'])
  
      try:
        # find all the y parts and add them up, find all the x parts and add them up, take the square root
        ypart = (sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'], results2[each_glitch]['val'])])/num_valid_obs)**2 
        
        xpart = (sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'],results2[each_glitch]['val'])])/num_valid_obs)**2 
        
        glitched_mag = math.sqrt(ypart + xpart)
      
      except Exception:

        # when some of the values are none, only do the values we need
        num_valid_obs = len([x for x in results1[each_glitch]['val'] if x != None and x != 'None'])

        ypart = (sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'], results2[each_glitch]['val']) if speed != 'None' and x != 'None'])/num_valid_obs)**2  
        xpart = (sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'],results2[each_glitch]['val']) if speed != 'None' and x != 'None'])/num_valid_obs)**2 
        
        glitched_mag = math.sqrt(ypart + xpart)

      try:
        num_flags = len(results_flags[each_glitch]['fval'])
        
        if 'E' not in results[each_glitch]['fval'] and 'M' not in results[each_glitch]['fval'] and 'Q' not in results[each_glitch]['fval']:

          glitched_spd_flag = 'A'
        
        else:
          numM = len([x for x in results_flags[each_glitch]['fval'] if x == 'M'])
          numE = len([x for x in results_flags[each_glitch]['fval'] if x == 'E'])
          numQ = len([x for x in results_flags[each_glitch]['fval'] if x == 'Q'])

          if numM/num_flags > 0.8:
            glitched_mag_flag = 'M'
          elif numE/num_flags > 0.05:
            glitched_mag_flag = 'E'
          elif (numE + numM + numQ)/num_flags > 0.05:
            glitched_mag_flag = 'Q'
          else:
            glitched_mag_flag = 'A'
      
      except Exception:
          glitched_mag_flag = 'M'

    elif results1[each_glitch]['val'] == [] or results2[each_glitch]['val'] == []:
      glitched_mag = None
      glitched_mag_flag = 'M'

      # throw b or n flag if speed or mag is less than detection limits
                    
    if glitched_mag < 1.0 and glitched_mag > 0.3:
        glitched_mag_flag = "B"
    elif glitched_mag <= 0.3:
        glitched_mag_flag = "N"
    else:
        pass
      
    final_glitch[each_glitch] = {'mean': round(glitched_mag,2), 'flags': glitched_mag_flag}

  return final_glitch

def create_glitched_dirs(results1, results2):
  """ Campbell uses the yamartino method, which is a weighted direction based on speed"""
  final_glitch = {}

  for each_glitch in sorted(results1.keys()):
    if results1[each_glitch]['val'] != []:

      num_valid_obs = len(results1[each_glitch]['val'])

      # computes the wind direction
      try:

        theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'], results2[each_glitch]['val'])])/num_valid_obs, sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'],results2[each_glitch]['val'])])/num_valid_obs)

        glitched_dir = round(math.degrees(theta_u),3)

      except Exception: 

        # when some of the values are none, only do the values we need
        num_valid_obs = len([x for x in results1[each_glitch]['val'] if x != None and x != 'None'])

        # computes the wind direction
        theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'], results2[each_glitch]['val']) if speed != 'None' and x != 'None'])/num_valid_obs, sum([float(speed) * math.cos(math.radians(float(x))) for (speed, x) in itertools.izip(results1[each_glitch]['val'],results2[each_glitch]['val']) if speed != 'None' and x != 'None'])/num_valid_obs)

        glitched_dir = round(math.degrees(theta_u),3)

      try:
        num_flags = len(results_flags[each_glitch]['fval'])
        if 'E' not in results_flags[each_glitch]['fval'] and 'M' not in results_flags[each_glitch]['fval'] and 'Q' not in results_flags[each_glitch]['fval']:
          flagged_val = 'A'
        else:
          numM = len([x for x in results_flags[each_glitch]['fval'] if x == 'M'])
          numE = len([x for x in results_flags[each_glitch]['fval'] if x == 'E'])
          numQ = len([x for x in results_flags[each_glitch]['fval'] if x == 'Q'])

          if numM/num_flags > 0.8:
            flagged_val = 'M'
          elif numE/num_flags > 0.05:
            flagged_val = 'E'
          elif (numE + numM + numQ)/num_flags > 0.05:
            flagged_val = 'Q'
          else:
            flagged_val = 'A'
    
      except Exception:
          flagged_val = 'M'

    elif results1[each_glitch]['val'] == [] or results2[each_glitch]['val'] == []:
      meanval = None
      flagged_val = 'M'

    final_glitch[each_glitch] = {'mean': round(glitched_dir,2), 'flags': flagged_val}

  return final_glitch



def csv_that_glitch(final_glitch, csvfilename = "sample_glitch_csv.csv"):
  """ create a csv for the glitch. pass 1 parameter of csvfilename = <blah> if you want to name it"""
  csv_list = []

  with open(csvfilename, 'wb') as writefile:
    writer = csv.writer(writefile)

    # raw dates is type date time and dates is string type; we need the raw type to search the dictionary and the string type to write the output
    raw_dates = [x for x in sorted(final_glitch.keys())]
    dates = [datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S') for x in sorted(final_glitch.keys())]
    
    try:
      vals= [final_glitch[x]['mean'] for x in dates]
    except Exception:
      vals = [final_glitch[x]['mean'] for x in raw_dates]
    t
    ry:
      flags = [final_glitch[x]['flags'] for x in dates]
    except Exception:
      flags = [final_glitch[x]['flags'] for x in raw_dates]

    writer.writerow(['DATE_TIME', 'GLITCHED_MEAN', 'GLITCHED_FLAG'])
    csv_list.append("<br> DATE_TIME, GLITCHED_MEAN, GLITCHED_FLAG </br>")
    
    for index, each_date in enumerate(dates):
      writer.writerow([each_date, vals[index], flags[index]])
      csv_list.append("<br>" + each_date + "," + str(vals[index]) +", " + flags[index] +"</br>")

  print("Glitched csv -- standard formatting -- completed!")
  return csv_list 

def csv_that_windy_glitch(final_glitch1, final_glitch2, final_glitch3, csvfilename="sample_windy_glitch_csv.csv"):
  """ with the wind we give it first speed (final glitch1) , then direction (final glitch2), then magnitude (final glitch3) """
  
  #import pdb; pdb.set_trace()
  csv_list = []

  with open(csvfilename, 'wb') as writefile:
    writer = csv.writer(writefile)

    raw_dates = [x for x in sorted(final_glitch1.keys())]
    dates = [datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S') for x in sorted(final_glitch1.keys())]

    try:
      vals_spd= [final_glitch1[x]['mean'] for x in dates]
    except Exception:
      vals_spd= [final_glitch1[x]['mean'] for x in raw_dates]

    # speed actually needs to take the flags from magnitude because these contain the N and B values
    try:
      flags_spd = [final_glitch3[x]['flags'] for x in dates]
    except Exception:
      flags_spd = [final_glitch3[x]['flags'] for x in raw_dates]

    try:
      vals_dir= [final_glitch2[x]['mean'] for x in dates]
    except Exception:
      vals_dir= [final_glitch2[x]['mean'] for x in raw_dates]

    try:
      flags_dir = [final_glitch2[x]['flags'] for x in dates]
    except Exception:
      flags_dir = [final_glitch2[x]['flags'] for x in raw_dates]

    try:
      vals_mag = [final_glitch3[x]['mean'] for x in dates]
    except Exception:
      vals_mag = [final_glitch3[x]['mean'] for x in raw_dates]

    try:
      flags_mag = [final_glitch3[x]['flags'] for x in dates]
    except Exception:
      flags_mag = [final_glitch3[x]['flags'] for x in raw_dates]

    writer.writerow(['DATE','GLITCHED_SPD_MEAN','GLITCHED_SPD_FLAG','GLITCHED_DIR_MEAN','GLITCHED_DIR_FLAG','GLITCHED_MAG_MEAN','GLITCHED_MAG_FLAG'])
    csv_list.append("<br> DATE, GLITCHED_SPD_MEAN, GLITCHED_SPD_FLAG, GLITCHED_DIR_MEAN, GLITCHED_DIR_FLAG, GLITCHED_MAG_MEAN, GLITCHED_MAG_FLAG </br>")

    for index, each_date in enumerate(dates):
      print index
      print each_date
      writer.writerow([each_date, vals_spd[index], flags_spd[index], vals_dir[index], flags_dir[index], vals_mag[index], flags_mag[index]])

      csv_list.append("<br>" + each_date + ", " + str(vals_spd[index]) +", " + str(flags_spd[index]) + ", " + str(vals_dir[index]) + ", " + str(flags_dir[index]) + ", " + str(vals_mag[index]) + ", " + str(flags_mag[index]) +"</br>")

    print csv_list
  print("Windy Glitch csv -- completed!")
  return csv_list

def csv_that_sonic_glitch(final_glitch1, final_glitch2, final_glitch3, final_glitch4, final_glitch5, csvfilename="sample_sonic_glitch_csv.csv"):
  """ when glitcher gets a sonic glitch, this method will handle it """

  csv_list = []

  with open(csvfilename, 'wb') as writefile:
    writer = csv.writer(writefile)
    writer.writerow(['DATE','GLITCHED_SPD_MEAN','GLITCHED_SPD_FLAG','GLITCHED_DIR_MEAN','GLITCHED_DIR_FLAG','GLITCHED_UX_MEAN','GLITCHED_UX_FLAG', 'GLITCHED_UY_MEAN', 'GLITCHED_UY_FLAG', 'GLITCHED_AIR_MEAN', 'GLITCHED_AIR_FLAG'])
    csv_list.append("<br> DATE, GLITCHED_SPD_MEAN, GLITCHED_SPD_FLAG, GLITCHED_DIR_MEAN, GLITCHED_DIR_FLAG, GLITCHED_UX_MEAN, GLITCHED_UX_FLAG, GLITCHED_UY_MEAN, GLITCHED_UY_FLAG, GLITCHED_AIR_MEAN, GLITCHED_AIR_FLAG </br>")
    
    raw_dates = [x for x in sorted(final_glitch1.keys())]
    dates = [datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S') for x in sorted(final_glitch1.keys())]

    try:
      vals_spd= [final_glitch1[x]['mean'] for x in dates]
    except Exception:
      vals_spd= [final_glitch1[x]['mean'] for x in raw_dates]

    try:
      flags_spd = [final_glitch1[x]['flags'] for x in dates]
    except Exception:
      flags_spd = [final_glitch1[x]['flags'] for x in raw_dates]

    try:
      vals_dir= [final_glitch2[x]['mean'] for x in dates]
    except Exception:
      vals_dir= [final_glitch2[x]['mean'] for x in raw_dates]

    try:
      flags_dir = [final_glitch2[x]['flags'] for x in dates]
    except Exception:
      flags_dir = [final_glitch2[x]['flags'] for x in raw_dates]

    try:
      vals_ux= [final_glitch3[x]['mean'] for x in dates]
    except Exception:
      vals_ux= [final_glitch3[x]['mean'] for x in raw_dates]

    try:
      flags_ux = [final_glitch3[x]['flags'] for x in dates]
    except Exception:
      flags_ux = [final_glitch3[x]['flags'] for x in raw_dates]

    try:
      vals_uy= [final_glitch4[x]['mean'] for x in dates]
    except Exception:
      vals_uy= [final_glitch4[x]['mean'] for x in raw_dates]

    try:
      flags_uy = [final_glitch4[x]['flags'] for x in dates]
    except Exception:
      flags_uy = [final_glitch4[x]['flags'] for x in raw_dates]

    try:
      vals_air= [final_glitch5[x]['mean'] for x in dates]
    except Exception:
      vals_air= [final_glitch5[x]['mean'] for x in raw_dates]

    try:
      flags_air = [final_glitch5[x]['flags'] for x in dates]
    except Exception:
      flags_air = [final_glitch5[x]['flags'] for x in raw_dates]

    for index, each_date in enumerate(dates):
      writer.writerow([each_date, vals_spd[index], flags_spd[index], vals_dir[index], flags_dir[index], vals_ux[index], flags_ux[index], vals_uy[index], flags_uy[index], vals_air[index], flags_air[index]])
      csv_list.append("<br>" + each_date + ", " + str(vals_spd[index]) + ", " + flags_spd[index] + 
        ", " + str(vals_dir[index]) + ", " + flags_dir[index] + ", " + str(vals_ux[index]) + ", " + flags_ux[index] + ", " + str(vals_uy[index]) + ", " + flags_uy[index] + ", " + str(vals_air[index]) + ", " + flags_air[index] + "</br>")

    print("Sonic Glitch csv -- completed!")
    return csv_list

def csv_that_solar_glitch(final_glitch1, final_glitch2):

  """ solar glitch first takes a mean and then a tot -- the difference is that precip is true on the second and false on the first, so that the total method is used"""
  
  #import pdb; pdb.set_trace()
  csv_list = []
  with open(csvfilename, 'wb') as writefile:
    writer = csv.writer(writefile)

    raw_dates = [x for x in sorted(final_glitch1.keys())]
    dates = [datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S') for x in sorted(final_glitch1.keys())]

    # solar mean
    try:
      vals_mean= [final_glitch1[x]['mean'] for x in dates]
    except Exception:
      vals_mean= [final_glitch1[x]['mean'] for x in raw_dates]

    
    try:
      flags_mean = [final_glitch1[x]['flags'] for x in dates]
    except Exception:
      flags_mean = [final_glitch1[x]['flags'] for x in raw_dates]

    # solar tot
    try:
      vals_tot= [final_glitch2[x]['mean'] for x in dates]
    except Exception:
      vals_tot= [final_glitch2[x]['mean'] for x in raw_dates]

    try:
      flags_tot = [final_glitch2[x]['flags'] for x in dates]
    except Exception:
      flags_tot = [final_glitch2[x]['flags'] for x in raw_dates]

    writer.writerow(['DATE','GLITCHED_RAD_MEAN','GLITCHED_RAD_MEAN_FLAG','GLITCHED_RAD_TOT','GLITCHED_RAD_TOT_FLAG','GLITCHED_MAG_MEAN','GLITCHED_MAG_FLAG'])
    csv_list.append("<br> DATE, GLITCHED_RAD_MEAN, GLITCHED_RAD_MEAN_FLAG, GLITCHED_RAD_TOT, GLITCHED_RAD_TOT_FLAG </br>")

    for index, each_date in enumerate(dates):
      print index
      print each_date
      writer.writerow([each_date, vals_mean[index], flags_mean[index], vals_tot[index], flags_tot[index]])

      csv_list.append("<br>" + each_date + ", " + str(vals_mean[index]) +", " + str(flags_mean[index]) + ", " + str(vals_tot[index]) + ", " + str(flags_tot[index]) +"</br>")

    print csv_list
  print("Solar csv -- completed!")
  return csv_list


def html_that_glitch(final_glitch):
  """ makes some lists"""

  list_of_dates=[datetime.datetime.strftime(glitch_day, '%Y-%m-%d %H:%M') for glitch_day in sorted(final_glitch.keys())]
  list_of_flags=[final_glitch[glitch_day]['flags'] for glitch_day in sorted(final_glitch.keys())]
  list_of_vals=[str(final_glitch[glitch_day]['mean']) for glitch_day in sorted(final_glitch.keys())]

  with open('//Volumes/andlter/LTERPlot/Fox/test.html','w') as htmlfile:
    htmlfile.write("""
    <!DOCTYPE html>
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <title>Test from Python</title>
    </head>
    <h1> Hello, this is Python, coming at you from APPLE. </h1>
    <body>
    <br> Date, Flag, Value </br>
    """)

    for index, item in enumerate(list_of_dates):
      new_row = [item, list_of_flags[index], list_of_vals[index], "</br>"]
      nr = ", ".join(new_row)
      htmlfile.write(nr)
    htmlfile.write("</body></html>")
    htmlfile.close()