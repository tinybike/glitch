READ ME -- Glitch.py
=========

Version: 0.1.0

Glitch.py is a Python implementation of the original GLITCH program implemented in Perl. The Perl script ran as a CGI script on our Cold Fusion page but now it does not work because many data structures have changed and become more complex, so glitch.py exists to bring back the old glitch functions. This "climate data summary tool" takes high resolution climate inputs, breaks them into one minute sections (for means, these sections are replications of the mean, for sums they are elements in that sum, and for wind magnitude they are x and y vectors, etc.) and then re-aggregates these components to a new time interval. 

So, no interval less than a minute can be processed, and although in theory one could process a large interval, more than a day is really very silly.

This repository contains several key python scripts: map_glitch.py, glitch.py, logic_glitch.py, and api_glitch.py (a front-end, front_glitch.py, is in development).

* map_glitch.py builds the lookup tables (json and yaml) from the database. 
* glitch.py is most of the "back end" functions which do the computation of the intervals and the reaggregation.
* logic_glitch.py helps to determine how various "methods" of glitching map onto our tables, and how to write outputs for them. logic_glitch.Glitcher classes contains most of the functionality, and logic_glitch.SmartGlitcher classes use the rules applied in Glitcher to do very high level functions like "tocsv".
* api-glitch.py is the very simple command line api.

map_glitch.py
------------

Maps the production database to find things to glitch. Unlike the old method, it avoids using entity and subentity, so hey! no more needing to predefine queries. There's a bunch of helper functions in there but basically you just need

            _,cursor = connect() <- the read-only connection

      That one is the connection string ^

            output1 = all_build_dict(cursor)

This one has an array, "possible", which is the possible dbcodes that contain probe and datetime. It just helps you limit your search. Right now I can see 4 possible tables, MS043, MS001, MS005, and HT004. However, more could be added.


      all_get_references(output1, cursor)

This one eats the output of the above, and can be further refined to fit the data. 

For example, we clean out entities that don't have DATE_TIME or DATETIME, but you might want to keep something like DT or TMSTAMP. We clean out ones that don't have MEAN as an attribute (because all MEAN have a MEAN FLAG with them right now), but you might want to keep around TOT or INST. Right now we also keep TOT, but as names at HJA change we may want to keep INST also.

The main method of map_glitch can be run executable. 

on awesome OS's, you just want to type:

            Not_a_PC:data_Ronin$ python map_glitch.py 

on lame windows boxes,

            C:/I/Just/Love/Bill/Gates/Python2.7/ python.exe map_glitch.py


It takes about 5 minutes to run this, so I'd recommend only doing it if you really have to. It's a long journey through all those tables.


glitch.py
-------

Glitch contains the main glitching functions.

(1) the connect() function connects to the database with the read only user

(2) the get_data(cursor, databaseid) function gets all the data tables associated
with a particular database code (such as MS043 or MS001)

(3) glitchme takes the received data and performs a glitching.

* First, it figures out what all the "found dates" are. For example, 2012-10-01 00:00:00, 2012-10-01 00:15:00, 2012-10-01 00:30:00, etc.

      * if a date is missing, it knows and doesn't include that in the list
      * the list is sorted ascending

* Then, it takes the first found date and the final found date and adds 1 "interval" you specify to that date. For example, if you want 2 hour intervals, the first date is 2012-10-01 02:00:00 in the above, and if it were to go to 2012-10-30 00:45:00, the last date would be 2012-10-30 02:45:00

* Then it makes an iterator for a range from that adjusted first value to the adjusted final value by the specified interval. The reason for the adjustment is because this iterator for the range represents the "checkpoints" we use to trigger an iteration. So we want to trigger the first iteration at the first stopping point. We need the iterator to run past the last interval, knowing that the interval will stop at its logical end because that is how we programmed it. 

      * If the first and final date you selected are length 1 (that is, there is only one single value selected, and it happens before the final date), then the program will ask you to go back and put in a longer interval. Because the first value would indicate the preceding intervals climate measurements, that measurement should not be distributed across the subsequent interval.

* Then it creates an iterator for a one-minute range between the first real date and the last real date. So, in this case, that iterator is for 2012-10-01 00:01:00, 2012-10-01 00:02:00, etc.

* Next, we create storage for the value of interest (called t_mean) and the flag of interest (called f_mean)

* Next, we step into the range of real dates. The current measurement is called this_date and the next measurement is called subsequent. The checkpoint we are looking for is called checkpoint.

* For most qualities, except totals, we are looking (at this stage) to replicate the value we find over the interval. 
      * For example, if the mean air temperature at 2012-10-02 00:15:00 is 11.7, this means that we have measurements of 11.7 temperature for the past 15 one minute intervals 2012-10-01 00:01:00 is 11.7, 2012-10-02 00:02:00 is 11.7 etc. Note that this is NOT linear interpolation. It is just replication. 
      * For wind magnitude, INITIALLY, we also just take these replicates, but when we go to re-assemble, we attach them to the direction appropriately. So, if at 2012-10-02 00:15:00 we had windspeed 2.0 and wind direction 145, that means at 2012-10-02 00:01:00 we had 2.0 and 145, at 2012-10-02 00:02:00 we had 2.0 and 145., etc. When we remake the magnitude we decompose each of those parts down into X and Y components, and, if the interval is longer than the given time, then those components are added by X and Y's to make an appropriate resultant.
      * For totals, like precip, if we measured 0.45 mm at 2012-10-02 00:15:00, this is the same as seeing 0.03 mm at 00:01:00, 0.03 at 00:02:00, etc. So each one minute in this case is divided by the duration of the interval.

* We iterate over the one minute intervals. 

      * First, we check if the interval is a checkpoint. If it is, and we haven't already seen it (that is bad), we take the list in the t_mean array (all the one minute values) and the list in the f_mean array (all the one minute flags) and we put it into the results map.

      * We reset the storage in t_mean and f_mean to blank and increment the checkpoint

      * We still need to make sure we get the value at that one-minute interval ready for the next append table, so we check:

            * Is the observation value still the same or does it also needed to be incremented?
            * Is the value a number or none?

      * We append the value, or a none, to the t_mean and f_mean. If we need to, we also increment the observation.

* When we run out of one-minute intervals, the iteration stops, and we return the glitch.


(4) create_glitch does glitch aggregation for 1 variable means and totals. Basically, it walks over the sorted dates in the output from glitchme, and either takes a mean of all the values in t_mean or takes their sum if a total. It also applies the flagging algorithm if the data is missing. If the missing data is more than 80 percent of the data, then the data is set to None.

(5) create_glitch_mags decomposes the wind speeds and wind directions to their x and y components, and computes the directional magnitude by adding the one-minute y and x values within that interval and taking the root of them. It also applies the glitch flagging algorithm, including setting the missing data to None.

(6) create_glitched_dirs decomposes the direction using the Yamartino method. The campbell loggers also use the Yamartino method-- it weights the directions by their speeds. This puts more trust on faster windspeeds, which makes sense on a prop.

(7) csv_that_glitch, csv_that_windy_glitch, csv_that_sonic_glitch, and csv_that_solar_glitch take the inputs from the glitch creations and make them into csv files that are written with appropriate headers. right now they make html files as well. This will be taken into another function. 


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


api_glitch.py
----------

api_glitch is a simple command line interface which will walk you through creating an instance of SmartGlitcher. 



Manual Tests of Function
---------------------

Several tests of function have been created. These will be included here on the next push.


Differences from the Perl
-------------
* glitchme can move both up and down in time, to 1 minute, and can also deal with non-1440 values. If the value is not a 1440 multiple, the last interval which would transgress the stop-time bound is not computed.

* create_glitched_output (and friends) creates a dictionary structure that can be easily read to a various structures for the web/making a csv. So far we have csv, html, and json. Tt's format is: {date_time of glitch: {'mean': mean value, 'flag': flag value}}

