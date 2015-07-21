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

-- csv will make you a csv file, which you can save anywhere
-- html will generate html, and will perhaps also update an html file on narra.


Manual Tests of Function
---------------------

1. Air Temperature, rather new


            A = SmartGlitcher('MS04311','AIRCEN01','2012-10-01 00:00:00','2012-10-07 00:05:00', 20)

* First Value:

            2012-10-01 00:20:00,12.73,A

In data: 

            2012-10-01 00:00:00, 12.7
            2012-10-01 00:15:00, 12.8

12.7*15 + 12.8*5 = 254.5 / 20. = 12.725 OKAY


* Another Value

            2012-10-01 00:40:00, 12.95, A

In data:

            2012-10-01 00:15:00 12.8
            2012-10-01 00:30:00 13.1

12.8*10 + 13.1*10 = 259
259/20 = 12.95 OKAY

* Another Value

            2012-10-06 18:20:00,13.8,A

In data:

            2012-10-06 18:00:00, 13.9
            2012-10-06 18:15:00, 13.5

13.9*15 + 13.5*5 = 13.8 OKAY


Older Air Temperature
-------------

A = SmartGlitcher('MS04311','AIRPRI03','2009-10-01 00:00:00','2009-10-07 00:05:00', 180)

            2009-10-01 03:00:00,7.92,A

From raw data: 8.0*15 + 8.0*15 + 7.9*15 + 7.8*15 + 7.8*15 + 7.9*15 + 7.9*15 +7.9*15 + 8.0*15 + 8.0*15 + 8.0*15 + 7.9*15 = 1421/180. = 7.92 OKAY


Shortened values for air temperature - what if glitch < actual measurement?
---------

A = SmartGlitcher('MS04311','AIRUPL01','2004-10-01 00:00:00','2004-10-03 00:05:00', 10)

* Old Data

            AIRUPL01    Oct  2 2004 12:00:00:000AM    11.6  A     NA    
            AIRUPL01    Oct  2 2004 12:15:00:000AM    11.7  A     NA    
            AIRUPL01    Oct  2 2004 12:30:00:000AM    11.7  A     NA    
            AIRUPL01    Oct  2 2004 12:45:00:000AM    11.7  A     NA    
            AIRUPL01    Oct  2 2004 01:00:00:000AM    11.8  A     NA   

* New Data

            2004-10-02 00:10:00,11.6,A
            2004-10-02 00:20:00,11.65,A
            2004-10-02 00:30:00,11.7,A
            2004-10-02 00:40:00,11.7,A

* Value at 10 minutes is 11.6 replicated 10 x OKAY
* Value at 20 minutes is 11.6 replicated 5 x and 11.7 replicated 5 x OKAY
* Value at 30 minutes is 11.7 replicated 10 x OKAY
* Value at 40 minutes is 11.7 replicated 10 x OKAY
  
  
Wind Test
--------

A = SmartGlitcher('MS04314','WNDCEN01','2010-10-01 00:00:00','2010-10-03 00:05:00', 10)

* Old Data

            WNDCEN01    Oct  1 2010 12:00:00:000AM 1.0, A   1.0, A, 64.5, A   
            WNDCEN01    Oct  1 2010 01:00:00:000AM  0.9, B  0.9, B      59.3, A       
            WNDCEN01    Oct  1 2010 02:00:00:000AM  0.9, B  0.8   B     56.6  A     18.7  A     NA    
      
* New Data

            2010-10-01 00:10:00,1.0,A,64.5,A,1.0,A
            2010-10-01 00:20:00,1.0,A,64.5,A,1.0,A
            2010-10-01 00:30:00,1.0,A,64.5,A,1.0,A
            2010-10-01 00:40:00,1.0,A,64.5,A,1.0,A

Value at 10, 20, 30, 40 minutes is 1.0 speed, 1.0 mag, 64.5 dir replicated 10x, ok... all values are this

            2010-10-01 02:00:00,0.9,B,59.3,A,0.9,B
            2010-10-01 02:10:00,0.9,B,56.6,A,0.9,B
            2010-10-01 02:20:00,0.9,B,56.6,A,0.9,B
            2010-10-01 02:30:00,0.9,B,56.6,A,0.9,B
            2010-10-01 02:40:00,0.9,B,56.6,A,0.9,B
            
Values at 10, 20, 30, 40 minutes is also correct replication of the hr data.



Wind Test 100 minutes
--------

A = SmartGlitcher('MS04314','WNDCEN01','2010-10-01 00:00:00','2010-10-03 00:05:00', 100)

      * Computed Values

            >>> 360-142.66
            217.34
            >>> 220.1*60 + 213.2*40
            21734.0

      * Old Data

            MS001 14    CENMET      WND008      1000  2A    WNDCEN01    Oct  1 2010 01:00:00:000PM    0.9   B     0.6   B     213.2 A     51.4  A     NA    
            MS001 14    CENMET      WND008      1000  2A    WNDCEN01    Oct  1 2010 02:00:00:000PM    0.9   B     0.5   B     220.1 A     53.7  A     NA    


Precip Test, 45 minutes, from 5 minute data -- see what happens with kind of old precip
-----------

B = SmartGlitcher('MS04313','PPTPRI01','2011-02-27 00:00:00','2011-03-02 00:00:00',45)



* Old Data

            PPTPRI01    Feb 27 2011 04:30:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 04:35:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 04:40:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 04:45:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 04:50:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 04:55:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 05:00:00:000AM    0.25  A     NA    
            PPTPRI01    Feb 27 2011 05:05:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 05:10:00:000AM    0.00  A     NA    
            PPTPRI01    Feb 27 2011 05:15:00:000AM    0.00  A     NA  

In the entire contributing interval we have 0.25 coming in for the total of one five minute interval. That means that each minute in that interval is giving 0.05  

            >>> bob[datetime.datetime(2011,2,27,5,15)]
            {'val': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.05, 0.05, 0.05, 0.05, 0.05, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 'fval': ['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A']}

The final output for this interval reflects only that sum of precipitation:

* New Data:

            2011-02-27 04:30:00,0.0,A
            2011-02-27 05:15:00,0.25,A


Precip Test 2, 15 minutes, from h15 crazy data
--------

* New Data: 

            2014-12-30 13:15:00,0.0,A
            2014-12-30 13:30:00,0.13,A
            2014-12-30 13:45:00,0.0,A


* old Data: 

            PPTH1502    Dec 30 2014 01:15:00:000PM    0.08  A     NA    
            PPTH1502    Dec 30 2014 01:20:00:000PM    0.05  A     NA    
            PPTH1502    Dec 30 2014 01:25:00:000PM    0.00  A     NA  


The glitch sees the sum of 0.08 and 0.05 to 0.13!


Radiation Test
----------

* Old Data:

            RADVAN01    Sep  1 2012 12:00:00:000PM    0.780 A     867   A     NA 
            RADVAN01    Sep  1 2012 12:15:00:000PM    0.779 A     865   A     NA    
            RADVAN01    Sep  1 2012 12:30:00:000PM    0.777 A     863   A     NA    
            RADVAN01    Sep  1 2012 12:45:00:000PM    0.772 A     858   A     NA    
            RADVAN01    Sep  1 2012 01:00:00:000PM    0.762 A     847   A     NA    
            RADVAN01    Sep  1 2012 01:15:00:000PM    0.752 A     836

New Data: 

            2012-09-01 12:11:00,854.12,A,2.2,A
            2012-09-01 12:54:00,863,A,2.23,A


            >>> 867 * 4 + 865 * 15 + 863 * 15 + 858 * 9
            37110
            >>> 37110/43
            863

Sonic Test
---------

* Old Data

            MS001 34    VANMET      WND011      1000  1A    WNDVAN02    Apr 10 2014 12:05:00:000AM    1.10  A     1.6   A     1.5   A     4.1   A     -1.1  A     0.2   A     0.0   A     0.1   A     2.2   A     0.4   A     NA    
            MS001 34    VANMET      WND011      1000  1A    WNDVAN02    Apr 10 2014 12:10:00:000AM    0.95  A     1.5   A     3.5   A     3.3   A     -0.9  A     0.1   A     0.1   A     0.1   A     2.0   A     0.4   A     NA    
            MS001 34    VANMET      WND011      1000  1A    WNDVAN02    Apr 10 2014 12:15:00:000AM    1.04  A     1.5   A     2.4   A     3.8   A     -1.0  A     0.1   A     0.0   A     0.1   A     1.8   A     0.3   A     NA    
            MS001 34    VANMET      WND011      1000  1A    WNDVAN02    Apr 10 2014 12:20:00:000AM    1.20  A     1.4   A     13.6  A     2.5   A     -1.2  A     0.1   A     0.3   A     0.1   A     1.8   A     0.3   A     NA    


* New Data

      2014-04-10 00:10:00,1.09,A,3.78,A,-1.1,A,0.05,A,2.1,A



Differences from the Perl
-------------
* glitchme can move both up and down in time, to 1 minute, and can also deal with non-1440 values. If the value is not a 1440 multiple, the last interval which would transgress the stop-time bound is not computed.

* create_glitched_output (and friends) creates a dictionary structure that can be easily read to a various structures for the web/making a csv. So far we have csv, html, and json. Tt's format is: {date_time of glitch: {'mean': mean value, 'flag': flag value}}

* glitch.py can do graphics, csv, or html. This means it can be incorporated into Hans' scripts more easily for the "Cold Fusion"

* glitch.py has a simple command line interface as an alternative to just the web solution

* glitch.py maps the database into simple folders you can use to know the names of probes, etc. It never uses the intermediate tables but instead relies on entity/attribute, etc. so that as probes are added these changes can be passed to glitch.py by simply running map.py prior to running it. one could even batch the running of map.py on the shared drive so that there would always be an up to date copy. it takes about 5 minutes to run map.py because it is checking and re-indexing the whole db. But it will not bog up the connection because it uses a pooling.

Glitch.py follows a creative-commons attribution 2.0 share-alike voluntary license. You are free to share, copy, transmit, or adapt the program, but you must provide attribution to Fox Peterson and the Share Alike license. 
