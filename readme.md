READ ME -- NewGlitch
=========

Version: 0.0.5

NewGlitch is a Python implementation of the original GLITCH program implemented in Perl. We felt that the original Perl program was pretty damn good, but unfortunately, with all the data we have and some inconsistencies in naming, the original program could not keep up with some breaking changes. I tried originally to just fix the Perl, but that turned out to be more difficult than I could handle and come to a solution, so NewGlitch was born.

NewGlitch contains 2 key python scripts: glitch.py and logic_glitch.py. glitch.py is most of the "back end" functions which do the computation. logic_glitch.py contains a few useful functions, but mostly two classes, Glitcher and SmartGlitcher. SmartGlitcher is a "smart" output tool for Glitcher.  You could use just glitch.py to compute the glitches in a raw way, use just Glitcher to compute each attribute, or use SmartGlitcher to help you make nice clean outputs when you have evil input structures, like, uhm, sonic anemometers.

Components
------

map_glitch.py - generates .yaml and .json files that map the glitcher program onto any possible data set it could use in our database. Right now we find about 7 data sets, although some of them, like TW006, aren't very interesting.
glitch.py - does the maths
front_glitch.py - uses the bottle framework to dynamically route requests -- in progress
logic_glitch.py - controls the mapping of glitch onto yaml files to reduce the inputs needed-- i.e. instead of you needing to figure out, can I glitch this, and what attributes from it are glitchable, you can just use logic_glitch's Glitcher class and SmartGlitcher class to handle this, and if it seems out of date, just execute map_glitch.py

map_glitch.py
------------

Basically, it maps the production database to find things to glitch. Unlike the old method, it avoids using entity and subentity, so hey! no more needing to predefine queries. There's a bunch of helper functions in there but basically you just need

            _,cursor = connect() <- the read-only connection

      That one is the connection string ^

            output1 = all_build_dict(cursor)

      This one has an array, "possible", which is the possible dbcodes that contain probe and datetime. It just helps you limit your search.


            all_get_references(output1, cursor)

      This one eats the output of the above, and can be further refined to fit the data. For example, we clean out entities that don't have DATE_TIME or DATETIME, but you might want to keep something like DT or TMSTAMP. We clean out ones that don't have MEAN as an attribute (because all MEAN have a MEAN FLAG with them right now), but you might want to keep around TOT or INST or something. You could add this if you wanted.


The main method of map_glitch can be run executable. 

on awesome OS's, you just want

Not_a_PC:data_Ronin$ python map_glitch.py 

on lame windows boxes,

C:/I/Just/Love/Bill/Gates/Python2.7/ python.exe map_glitch.py


glitch.py
-------

Glitch is where the magic happens. 

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

      csv_that_glitch(finalz)
      html_that_glitch(finalz)

      print finalz


(1) the connect() function connects to the database with the read only user

(2) the get_data(cursor, databaseid) function gets all the data tables associated
with a particular database code (such as MS043 or MS001)

(3) the get_probes(cursor, database_dictionary) function chains on the output of the
get_data function to get all the probe codes for that data base (i.e. MS043, we would get a structure with 'MS04301': {AIRCEN01, AIRPRI01, etc.}, and then 'MS0405': {'RADCEN01', 'RADVAN01', etc.})

(4) the get_probetype(cursor, tablename) function is right now being implemented on just MS04311 for testing. However, at this point we could loop over all the keys in the probe dictionary to get a list of appropriate names for a larger comprehension. I figured in the final implementation, this function would be called based on knowing which table we were after, so a loop would not be needed here.

(5) the get_attribute_names(cursor, tablename) function gets the name of the first attribute to contain "mean" right now. It is set that we could add more like the wind if needed. it also gets the matching flag.

(6) the get_data_in_range function gets the data for a given probe. In this case, I pass it just one probe name, 'AIRCEN01'. In the user case, that would be an open parameter supplied by a list generated from the output of the get_probes function. 

(7) glitchme takes the output from get_data_in_range and performs the dis-aggregation. See the comments in the code. Basically we generate a range of one minute time stamps, and use Kyle's method to replicate the last value measured until the next value measured along those one minute time steps. A generator expression is made to find the "times to glitch to". We iterate over the one minute values, adding them to a temporary list until we reach the next "interval to glitch to". Then we store the time of the interval and the list of values (and their flags) in an output structure. We generate the next value to glitch to, and continue to move along the one minute values. glitchme arrays are nice because they can be used with any of the summary methods (wind, regular, etc.)

(8) for almost everything but wind speed and direction, you just need to use create_glitched_output to generate means and flags. 

(9) for wind speeds, you will want to run glitchme on the wind speed, and on the wind direction, and then pass it the wind speed, wind direction, and wind speed flags. This can be more easily handled in a Glitcher instance, where it's taken care of for you. then you can create_glitched_speeds to get an output

(10) for wind direction, you will run glitch me on wind speed, wind direction, and then pass it the wind speed, wind direction, and wind direction flags. This is also easier to handle in a Glitcher instance. Otherwise, use create_glitched_dirs to get the right output

logic_glitch.py
----------

The Glitcher class makes glitching easier. The SmartGlitch class helps you write Glitchers and csv outputs.

Call Glitcher like this:


            instanceName = Glitcher(table, name, startdate, enddate, interval)

for example:

            instanceName = Glitcher('MS04311','AIRCEN01','2014-01-01 00:05:01', '2014-02-01 00:07:13', 321)

That ridiculous example would do a glitch on air temperature at CENMET at a 321 minute glitch, from the first passed time to the final passed time.

You can Glitcher even on wind and sonic

            instanceName = Glitcher('MS04334', 'WNDVAN02', '2014-03-02 00:00:00', '2014-03-12 00:00:00', 200)

But of course, you have to pass it the name of a probe/sonic that is appropriate. It will be smart about the date not being in the right range and just bring back emptiness, but the user input tools should control probe name as I understand them?

At this point, you are ready for the glitch, but you need to make sure it outputs correctly from either a normal glitch, a wind glitch, or a sonic glitch, so call

            outputName = instanceName.decide()

outputName is either a dictionary or a "three-ple" or "five-ple" of dictionaries containing date objects, means, and flags. I.e. the create_glitched_outputs outputs, but brought together for the wind and sonice.

It might be easy to write the csvs by calling each of these components of the three-ple or five-ple to csv_that_glitch in glitch, but to make it easier, even, just use the SmartGlitcher class


The SmartGlitcher class supers the Glitcher class. Call it like this:

      instanceName = SmartGlitcher('MS04314', 'WNDPRI01', '2014-05-01 00:06:00', '2014-05-05 00:19:00', 250)
      outputName = instanceName.final_glitch()
      instanceName.tocsv(outputName, 'desired_csv_filename.csv')

If you don't specify a filename, it will give you some default name like "sample.csv".

Differences from the Perl
-------------
* glitchme can move both up and down in time, to 1 minute, and can also deal with non-1440 values. If the value is not a 1440 multiple, the last interval which would transgress the stop-time bound is not computed.

* create_glitched_output (and friends) creates a dictionary structure that can be easily read to a various structures for the web/making a csv. So far we have csv, html, and json. Tt's format is: {date_time of glitch: {'mean': mean value, 'flag': flag value}}

