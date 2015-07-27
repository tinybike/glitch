import csv
import pymssql
from random import randint
import datetime

def connect():
    ''' connect to the fsdbdata database'''

    conn = pymssql.connect(server = 'stewartia.forestry.oregonstate.edu:1433', user='ltermeta', password='$CFdb4LterWeb!')
    cursor = conn.cursor()

    return conn, cursor

def randomizer(year, interval_used, filename):

    # choose a random day between 1 and 28
    day_chosen = randint(1,28)
    # choose a random month between 1 and 12
    month_chosen = randint(1,12)
    # choose a random hour
    hour_chosen = randint(0,23)

    with open(filename, 'rb') as readfile:
        reader = csv.reader(readfile)
        reader.next()
        for row in reader:
            

            dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=interval_used)
            value = str(row[1])

            if dt.year == year and dt.month == month_chosen and dt.day == day_chosen and dt.hour == hour_chosen:
                print "match found :  " + (str(row[0])) + " to given : " + datetime.datetime.strftime(datetime.datetime(year, month_chosen, day_chosen, hour_chosen, 0), '%Y-%m-%d %H:%M:%S')
                print "value found : " + str(row[1])

    return str(row[1]), datetime.datetime(year, month_chosen, day_chosen, hour_chosen, 0)

def find_data(cur, random_date, random_data, probe_code):

    query = "select top 5 * from fsdbdata.dbo.ms04313 where probe_code like \'" + probe_code + "\' and date_time >= \'"  + datetime.datetime.strftime(random_date, '%Y-%m-%d %H:%M:%S') + "\'order by date_time asc"

    cur.execute(query)

    for row in cur:
        print row



    


if __name__ == "__main__":

    _, cur = connect()
    rd, rd2 = randomizer(2010, 5, 'ppt_pri_5min_2009_2011.csv')
    find_data(cur, rd2, rd, 'PPTPRI01')