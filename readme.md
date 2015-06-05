READ ME
=========

Glitch.py is a python interpretation of the fsdbdata glitch program.

Originally the intent was just to fix the perl, but actually this version will do the same as the Perl.

Right now I do not have an API set up, but here is how it works.

The main script at the bottom is an implementation of Glitch.py on a sample bit of data.

    if __name__ == "__main__":

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


* the connect() function connects to the database with the read only user

* the get_data(cursor, databaseid) function gets all the data tables associated
with a particular database code (such as MS043 or MS001)

* the get_probes(cursor, database_dictionary) function chains on the output of the
get_data function to get all the probe codes for that data base (i.e. MS043, we would get a structure with 'MS04301': {AIRCEN01, AIRPRI01, etc.}, and then 'MS0405': {'RADCEN01', 'RADVAN01', etc.})

* the get_probetype(cursor, tablename) function is right now being implemented on just MS04311 for testing. However, at this point we could loop over all the keys in the probe dictionary to get a list of appropriate names for a larger comprehension. I figured in the final implementation, this function would be called based on knowing which table we were after, so a loop would not be needed here.

* the get_attribute_names(cursor, tablename) function gets the name of the first attribute to contain "mean" right now. It is set that we could add more like the wind if needed. it also gets the matching flag.

* the get_data_in_range function gets the data for a given probe. In this case, I pass it just one probe name, 'AIRCEN01'. In the user case, that would be an open parameter supplied by a list generated from the output of the get_probes function. I.e. the get data function generates a list, and the list is used to write <li></li> tags around each name returned, which then is passed to a template engine to generate a drop down. Then this function can be called using the returned value from that drop down.

* glitchme takes the output from get_data_in_range and performs the aggregation. See the comments in the code. Basically we generate a range of one minute time stamps, and use Kyle's method to replicate the last value measured until the next value measured along those one minute time steps. A generator expression is made to find the "times to glitch to". We iterate over the one minute values, adding them to a temporary list until we reach the next "interval to glitch to". Then we store the time of the interval and the list of values (and their flags) in an output structure. We generate the next value to glitch to, and continue to move along the one minute values. At the end of the interval, we take the mean of the one minute values. This provides the weighted mean that Kyle computed.

* glitchme can move both up and down in time, to 1 minute, and can also deal with non-1440 values. If the value is not a 1440 multiple, the last interval which would transgress the stop-time bound is not computed.

* create_glitched_output creates a dictionary structure that can be easily read to a json structure for the web/making a csv. it's format is: {date_time of glitch: {'mean': mean value, 'flag': flag value}}


Now I must go to the andrews meeting. I hope this will help you. I am teaching myself flask and bottle, two python web frameworks, so I may be able to aid in porting this into the web.
