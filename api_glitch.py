#!usr/bin/python
import sys
import yaml
from glitch import * 
from logic_glitch import *

def describe_glitch():
    print """
    Welcome to Glitch.py, python-based climate data aggregation.
    
    ~ First, I will ask if you would like to remap your database. This 
    takes about 5 minutes, and will rebuild the yaml and json files
    which specify the available probes, their start and end dates,
    and the available "mean words" and "flag words". You should do this 
    any time you make a structural change to the database, or update/insert 
    a lot of new data.

    ~ Next, I will ask you to choose a table. From these tables, you will be 
    presented with a list of available probes.

    ~ Next, I will ask you to choose a probe. You will be shown a list of 
    available dates for that probe.

    ~ Next, you will choose your starting and ending dates, and the interval
    (in minutes) that you wish to glitch. Please do not select intervals 
    of less than a minute, or I will crash.

    ~ Finally, you can choose to have a csv output, html output, or graphical 
    output. For .csv, type --csv. For .html, type --html. For graphical, type 
    --png. You can choose any or all of these.
    """

def get_yaml():
    print "Please choose one of the following tables: MS043, MS001, MS005, HT004"
    table_name = raw_input("Type your selection here --> ")

    try:
        strfilename = table_name[0:5] + '.yml'
        u = yaml.load(open(strfilename,'r'))
        print "Success! Loaded " + table_name
        return u
    except Exception:
        print "Failure! Do you want to try again?"
        y_or_n = raw_input("Type YES for trying again or press CTRL + C to exit the program")
        if y_or_n == "YES":
            get_yaml()
        else:
            return False

def show_tables(loaded_yaml):
    """ displays possible tables and probes"""

    print('The possible tables are: ')
    for index, each_table in enumerate(sorted(loaded_yaml.keys())):
        print str(index) + ". |   " + each_table

    table_choice = raw_input('Choose one of the above tables--> ')

    # if they give you an index rather than a table name, search by the index

    if len(table_choice) == 1:
        table_choice_name = sorted(loaded_yaml.keys())[int(table_choice)]
        table_choice = table_choice_name
        del table_choice_name

    elif len(table_choice) == 7:
        pass

    else:
        print "That is not a valid table choice. Please try again or press CTRL + C to exit"
        print "---------"
        show_tables(loaded_yaml)


    # select a probe from the list of probes
    probe_choices = sorted(loaded_yaml[table_choice]['startdaydict'].keys())
    for index, item in enumerate(probe_choices):
        print str(index) + ". | " + item
    
    probe_choice = raw_input('Choose one of the above probes--> ')

    # if the probe number is given rather than the name
    if len(probe_choice) == 1:
        probe_choice_name = sorted(loaded_yaml[table_choice]['startdaydict'].keys())[int(probe_choice)]
        probe_choice = probe_choice_name
        del probe_choice_name

    elif len(probe_choice) == 8:
        pass
        print "you choose probe " + probe_choice

    else:
        print "That is not a valid probe choice. Please try again or press CTRL + C to exit"
        print "---------"
        show_tables(loaded_yaml)
            

    start_day = loaded_yaml[table_choice]['startdaydict'][probe_choice]
    end_day = loaded_yaml[table_choice]['enddaydict'][probe_choice]
    mean_words = loaded_yaml[table_choice]['mean_words']
    # method_words = loaded_yaml[table_choice]['method_words']
    probe_word = loaded_yaml[table_choice]['probe_word']
    date_word = loaded_yaml[table_choice]['date_word']

    return table_choice, probe_choice, start_day, end_day


def choose_dates(start_day, end_day):

    if type(start_day) != datetime.datetime:
        print "The earliest date we have is : " + str(start_day)
    else:
        print "The earliest date we have is : " + datetime.datetime.strftime(start_day, '%Y-%m-%d %H:%M:%S')

    if type(end_day) != datetime.datetime:
        print "The last date we have is: " + str(end_day)
    else:
        print "The last date we have is : " + datetime.datetime.strftime(end_day, '%Y-%m-%d %H:%M:%S')

    my_start_day = raw_input("Please choose a start date and type it as YYYY-mm-dd HH:MM:SS --> ")
    my_end_day = raw_input("Please choose an end date and type it as YYYY-mm-dd HH:MM:SS --> ")

    msd = datetime.datetime.strptime(my_start_day,'%Y-%m-%d %H:%M:%S')
    med = datetime.datetime.strptime(my_end_day, '%Y-%m-%d %H:%M:%S')

    if msd < datetime.datetime.strptime(start_day, '%Y-%m-%d %H:%M:%S'):
        msd = start_day
        print "That's too early, starting on the given start date"
    else:
        pass

    msd = datetime.datetime.strftime(msd, '%Y-%m-%d %H:%M:%S')

    if med > datetime.datetime.strptime(end_day, '%Y-%m-%d %H:%M:%S'):
        med = end_day
        print "That's too late, stopping on the given date"
    else:
        pass
    med = datetime.datetime.strftime(med, '%Y-%m-%d %H:%M:%S')

    return msd, med

def interval_choice():

    print "Please give an aggregation interval, in minutes, between 1 and 1440. You may choose longer, but not shorter. Longer is not recommended."

    interval = raw_input("Interval (integers only, between 1 and 1440) --> ")

    if int(interval) < 1 or int(interval) > 1440:
        print "That is not a valid input"
        interval_choice()

    else:
        return int(interval)



def what_to_do(thisGlitch):
    print("What do you want to do with your glitch?")

    possible_outputs = raw_input("Type --csv for CSV file, --png for Graph, --html for webpage. You can have all three! -->")

    if "--csv" in possible_outputs:
        csvfilename = raw_input("What to call your csv file? --> ")
        thisGlitch.tocsv(csvfilename)

    else:
        pass

if __name__ == "__main__":

    describe_glitch()
    myTable = get_yaml()
    myTableChoice, myProbeChoice, theStartDate, theEndDate = show_tables(myTable)
    myStartDay, myEndDay = choose_dates(theStartDate, theEndDate)
    myInterval = interval_choice()

    print('you have selected: ')
    print('''
        table : %s 
        probe : %s
        starts : %s
        ends : %s
        interval : %s
        '''
        ) %(myTableChoice, myProbeChoice, myStartDay, myEndDay, str(myInterval))
    
    thisGlitch = SmartGlitcher(myTableChoice, myProbeChoice, myStartDay, myEndDay, myInterval)

    what_to_do(thisGlitch)