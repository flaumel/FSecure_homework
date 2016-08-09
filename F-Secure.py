# coding=utf-8
import logging
import json
import database
import sqlite3
import os
import datetime
import time

def showMenu():
   print("1. Get information about all event times (in hours)")
   print("2. Get amount of app launches")
   print("3. Exit")
   option = input("What would you like to do?\n")
   logging.debug("Option chosen in menu is %s" % option)
   return option

'''
 Function flattening multi-level dictionary
 Arguments:
           data - dictionary to flatten
 Returns:
           Flattened dictionary
 Source: https://git.opnfv.org/cgit/yardstick/tree/yardstick/dispatcher/influxdb.py
'''
def _dict_key_flatten(data):
    next_data = {}

    if not [v for v in data.values()
            if type(v) == dict or type(v) == list]:
        return data

    for k, v in data.items():
        if type(v) == dict:
            for n_k, n_v in v.items():
                next_data["%s.%s" % (k, n_k)] = n_v
        elif type(v) == list:
            for index, item in enumerate(v):
                next_data["%s%d" % (k, index)] = item
        else:
            next_data[k] = v

    return _dict_key_flatten(next_data)

# Ask if user wants to create new database for events
var = input("Would you like to delete old database for events and create new one? (yes/no)\n")
if (var == 'yes'):
    os.remove('fSecure.db')
    logging.debug("Removed fSecure.db")

# Open connection to database
conn = sqlite3.connect('fSecure.db')
logging.debug("Database created and opened successfully")
c = conn.cursor()

# Ask if user wants to add events data to database
var = input("Would you like to add new events data into database (yes/no)?\n")
if (var == 'yes'):
    database.initializeDatabase(conn, c)

    lineNum = 1
    # Ask for filename with JSON events data
    var = input("From what file would you like to read JSON events data?\n")
    # Read file line by line, decode, flatten and insert into database
    with open(var, encoding="utf-8") as openfileobject:
        for line in openfileobject:
            decoded = json.loads(line)
            decoded = _dict_key_flatten(decoded)
            decoded_str = str(decoded)
            database.insert_event_data(conn, c, decoded)
            lineNum = lineNum + 1
            logging.basicConfig(filename='example.log', filemode='w',level=logging.DEBUG)
            if (lineNum % 10000 == 0):
                conn.commit()
                logging.warning('Processing event %s', lineNum)
    logging.warning("Done adding events to database!\n")
    conn.commit()
    
opt = showMenu()
while (opt != '3'):
    if (opt == '1'):
        logging.debug("Option 1")
        c.execute('SELECT timestamp FROM eventDATA')
        logging.warning("Getting data from eventData")
        timeAll = c.fetchall()
        hours = {}
        logging.warning("Interpreting data")
        for time in timeAll:
            hour = datetime.datetime.fromtimestamp(time[0]/1000).strftime("%H")
            if hour in hours:
                hours[hour] += 1
            else:
                hours[hour] = 1
        # Todo: print that dictionary in more user friendly way
        print(hours)
        opt = showMenu()
    elif (opt == '2'):
        logging.warning("Option 2")
        c.execute('SELECT COUNT(*) FROM appData')
        appAmount = c.fetchone()[0]
        appNumber = input("There is %s applications in database.\nWhich one launches count you want to get?\n" % appAmount)
        str = 'launch'
        c.execute('SELECT name FROM appData WHERE appId=?', [appNumber])
        name = c.fetchone()[0]
        c.execute('SELECT appId FROM eventData WHERE (eventType=? AND appID=?)', [str, appNumber])
        apps = c.fetchall()
        print("Application %s has been launched %s times" % (name, len(apps)))
        opt = showMenu()
    else:
        logging.warning("Else")
        opt = showMenu()