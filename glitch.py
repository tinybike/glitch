#!usr/bin/python

import pymssql
import time
import datetime
import itertools

def connect():
    ''' connect to the fsdbdata database'''

    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor

def get_data(cursor, databaseid):
  ''' from entity table, get the entity number and entity name based on the table name we are familiar with, such as MS043 '''

  dbid_dict = {}

  define_query = "select entity_id, entity_file_name from ltermeta.dbo.entity where entity_file_name like \'" + databaseid + "%\'"

  cursor.execute(define_query)

  for row in cursor:

    if str(row[1]).rstrip() not in dbid_dict:
      dbid_dict[str(row[1]).rstrip()] = str(row[0]).rstrip()
    else:
      pass

  return dbid_dict

def get_probes(cursor, dbid_dict):
  ''' from the sub-entity table, get the name of the probes which are associated with that particular table'''

  probe_dict = {}

  for tableid in dbid_dict.keys():

    define_query = "select entity_id, sub_entity_title from ltermeta.dbo.sub_entity where entity_id like \'" + dbid_dict[tableid] + "\'"

    cursor.execute(define_query)

    for row in cursor:

      if tableid not in probe_dict:
        probe_dict[tableid] = [str(row[1]).rstrip()]
      elif tableid in probe_dict:
        probe_dict[tableid].append(str(row[1]).rstrip())

  return probe_dict

def get_probetype(cursor, tableid):

  define_query = "select column_name from fsdbdata.information_schema.columns where table_name like \'" + tableid + "\' and column_name like \'PROBE%\'"

  cursor.execute(define_query)

  for row in cursor:
      probetype = str(row[0]).rstrip()

  return probetype

def get_attribute_names(cursor, tableid):

  attr = []
  flags = []

  define_query = "select column_name from fsdbdata.information_schema.columns where table_name like \'" + tableid + "\' and column_name like \'%MEAN%\'"
  print define_query

  cursor.execute(define_query)

  for row in cursor:
    if 'FLAG'.lower() not in str(row[0]).lower():
      mean_name = str(row[0]).rstrip()
      attr.append(mean_name)
    elif 'FLAG'.lower() in str(row[0]).lower():
      flag_name = str(row[0]).rstrip()
      flags.append(flag_name)
    else:
      print("attribute has a strange name of: %s") %(str(row[0]).rstrip())


  print attr, flags
  return attr, flags

def get_data_in_range(cursor, startdate, enddate, tableid, probetype, attr, flags, *args):

  valid_data = {}

  # temporary: get only the main "mean"
  attr0 = attr[0]
  flags0 = flags[0]

  if args and args != []:
    query = "select date_time, " + attr0 + ", " + flags0 + " from fsdbdata.dbo." + tableid + " where date_time >= \'" + startdate + "\' and  date_time < \'" + enddate + "\' and " + probetype + " like \'" + args[0] + "\'"

    cursor.execute(query)

    for row in cursor:
      if datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S') not in valid_data:
        valid_data[datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')] = {'attr': str(row[1]), 'flag': str(row[2])}
      elif datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S') in valid_data:
        print("duplicate value at : %s") %(str(row[0]))
  else:
    pass

  return valid_data

def drange(start, stop, step):
  ''' returns a date range generator '''
  r = start
  while r < stop:
      yield r
      r += step


def glitchme(valid_data, interval):
  ''' perform glitch aggregations LIKE A BOSS'''

  # Get all dates where we have valid data to key from
  dr = [x for x in sorted(valid_data.keys()) if valid_data[x] != 'None']

  # Generate a range of one minute intervals from start to end of desired period
  minrange = drange(dr[0], dr[-1], datetime.timedelta(minutes=1))

  # Get all values which have valid data
  val = [round(float(valid_data[x]['attr']),3) for x in sorted(valid_data.keys()) if valid_data[x] != 'None']

  # Get all flags associated with the valid data
  fval = [valid_data[x]['flag'] for x in sorted(valid_data.keys()) if valid_data[x] != 'None']

  # Define an iterator to move across the measurements, even if they are not the same in time length
  efficient_iterator = itertools.izip(dr[0:-1],dr[1:])

  # Compute the difference between each measurement and convert to minutes.
  tds = [(y-x).seconds/60 for (x,y) in efficient_iterator]


  # Special kind of interpolation in Glitch: chain together the tuples of number of 1 minute intervals * value of each, then zip this up to the date range at one min resolution, i.e. 1.8 at 1 min, 1.8 at 2 min, 1.8 at 3 min, 2.0 at 4 min, 2.0 at 5 min, etc.
  #one_minute_values = itertools.izip([p for p in minrange], list(itertools.chain.from_iterable([itertools.repeat(x,y) for (x,y) in itertools.izip(val[0:], tds)])))

  one_minute_values = itertools.izip([p for p in minrange],
  list(itertools.chain.from_iterable([itertools.repeat(x,y) for (x,y) in itertools.izip(val[0:], tds)])), list(itertools.chain.from_iterable([itertools.repeat(x,y) for (x,y) in itertools.izip(fval[0:], tds)])))

  """Now we have a value for every one minute interval- start with start time + 1 interval (for example if 45 minutes starting at noon the first stop point is 12:45), end with end time plus one interval (for example, if stop point is 10 pm and interval takes you to 10:45 this is the stop point), but don't go up to it if we don't complete that final interval (generator yields only until iteration must stop)"""

  # Generator for "stop points" for the new glitched interval i.e. every 35 minutes, etc.
  output_drange = drange(dr[0] + datetime.timedelta(minutes=interval), dr[-1] + datetime.timedelta(minutes=interval), datetime.timedelta(minutes=interval))

  # Each time the next method is called on the generator, we advance 1 stop point.
  this_date = output_drange.next()

  # for your sake, you can see the stop points (comment out if not helpful)
  print "sought date is " + str(this_date)

  # holds the resulting data for each stop point, persists
  results = {}
  results_flags = {}
  # holds the values to go to the mean for each stop point, is cleared between points
  t_mean = []
  # hold the flags to go to the mean for each stop point, and is cleared between points
  f_mean = []

  """ if the current minute is less than the stop point, we add its value to the minute table and add increment the time by 1 minute """
  for each_minute in one_minute_values:

    if each_minute[0] < this_date - datetime.timedelta(minutes=1):
      t_mean.append(each_minute[1])
      f_mean.append(each_minute[2])


    elif each_minute[0] == this_date-datetime.timedelta(minutes=1):
      t_mean.append(each_minute[1])
      results[this_date] = t_mean
      f_mean.append(each_minute[2])
      results_flags[this_date] = f_mean

      try:
        this_date = output_drange.next()
        print "sought glitched date-time is " + str(this_date)
        t_mean = []
        f_mean = []
      except StopIteration:
        print "Stop iteration caught, exiting the glitcher"
        results[this_date] = t_mean
        results_flags[this_date] = f_mean
        return results, results_flags

    elif each_minute[0] > this_date:
      print "output > date, unexpected behavior for iterator - see Fox to debug?"
      return results, results_flags

    else:
      print "Something unexpected. Hum X files theme. Existential crisis."

def create_glitched_output(results, results_flags):

    final_glitch = {}

    for each_glitch in sorted(results.keys()):

      if results[each_glitch] != []:

        meanval = round( sum(results[each_glitch])/len(glitched_dict[each_glitch]),2)

        try:
          num_flags = len(results_flags[each_glitch])
          if ['E','M','Q'] not in results_flags[each_glitch]:
            flaggedval = 'A'
          else:
            numM = len([x for x in results_flags[each_glitch] if x == 'M'])
            numE = len([x for x in results_flags[each_glitch] if x == 'E'])
            numQ = len([x for x in results_flags[each_glitch] if x == 'Q'])

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


      elif results[each_glitch] == []:
        meanval = None
        flaggedval = 'M'

      final_glitch[each_glitch] = {'mean': meanval, 'flags': flaggedval}

    return final_glitch

if __name__ == "__main__":

  # test with MS043, MS04311 - can loop over keys later
  cx, cur = connect()
  dbx_dict = get_data(cur, 'MS043')
  prx_dict = get_probes(cur, dbx_dict)
  probex = get_probetype(cur, 'MS04311')
  attrx, flagx = get_attribute_names(cur,'MS04311')
  vd = get_data_in_range(cur, '2010-04-10 00:00:00', '2010-04-10 02:00:00', 'MS04311', probex, attrx, flagx, 'AIRCEN01')

  # printing output for simple test
  print dbx_dict
  print prx_dict
  print vd

  # now to aggregate
  glitched_dict, glitched_flags = glitchme(vd, 3)

  finalz = create_glitched_output(glitched_dict, glitched_flags)

  print finalz
