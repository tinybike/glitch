#!usr/bin/python
import sys
import yaml
from glitch import * 
from logic_glitch import *


def hans_glitch(myTableChoice, myProbeChoice, myStartDay, myEndDay, myInterval):
    """  myTableChoice is like MS04311, MS00102, HT00401
         myProbeChoice is like AIRCEN01, PPTPRI01
         mySmartDay is like '2012-10-02 00:00:00'
         myEndDay is like '2014-10-01 06:05:00'
         myInterval is an INTEGER like 2, 24, 145
    """    

    thisGlitch = SmartGlitcher(myTableChoice, myProbeChoice, myStartDay, myEndDay, myInterval)

    csv_filename = "default_output.csv"
    htmlfilename = csv_filename[:-3] + "html"
    htmlfilename = thisGlitch.tocsv(csv_filename)

    print "<html><body>"
    print "".join(htmlfilename)
    print "</html><body>"


def describe_glitch():
    print """
    Welcome to Glitch.py, python-based climate data aggregation.
    
    ~ If you want to remap your database, you should run map_glitch.py first

    ~ Next, I will ask you to choose a table. From these tables, you will be 
    presented with a list of available probes.

    ~ Next, I will ask you to choose a probe. You will be shown a list of 
    available dates for that probe.

    ~ Next, you will choose your starting and ending dates, and the interval
    (in minutes) that you wish to glitch. Please do not select intervals 
    of less than a minute, or I will crash.

    ~ Finally, you can choose to have a cav and html output or just a graph. If you 
    want csv and html, the csv file and the html file will have the same name, but the html 
    file will save with the .html extension.
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

    possible_outputs = raw_input("Type csv for a csv and html, type png for a graph--> ")

    print("First I will display your graph in the browser. Press CTRL + c when you are done looking at it and I will save it to your directory under the name you choose next.")

    if "csv" in possible_outputs:
        csvfilename = raw_input("What to call your csv file? --> ")

        htmlfilename = csvfilename[:-3] + ".html"
        htmlfilename = thisGlitch.tocsv(csvfilename)

        print "do you also want to make a graph?"
        pngfilename = raw_input("if you want a graph, type a filename, otherwise, press CTRL + C to exit --> ")
        thisGlitch.graphme(pngfilename)

    else:
        pass

    if "png" in possible_outputs:
        pngfilename = raw_input("what to call your figure? --> ")
        thisGlitch.graphme(pngfilename)
    else:
        pass


if __name__ == "__main__":

    myTableChoice = sys.argv[1]
    myProbeChoice = sys.argv[2]
    myStartDay = sys.argv[3]
    myEndDay = sys.argv[4]
    myInterval = int(sys.argv[5])

    hans_glitch(myTableChoice, myProbeChoice, myStartDay, myEndDay, myInterval)