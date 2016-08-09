# coding=utf-8
import logging
import json
import database
import sqlite3
import os
import datetime
import time

# Source: https://git.opnfv.org/cgit/yardstick/tree/yardstick/dispatcher/influxdb.py
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

var = input("Would you like to make new database for events (yes/no)?\n")
#if (var == 'yes'):
#    os.remove('fSecure.db')
#    logging.debug("Removed fSecure.db")
#conn = sqlite3.connect('fSecure.db')
conn = sqlite3.connect(':memory:')
logging.debug("Database created and opened successfully")
c = conn.cursor()
database.initializeDatabase(conn, c)

lineNum = 1
#var = input("Would you like to add more data 

with open('obfuscated_data', encoding="utf-8") as openfileobject:
    for line in openfileobject:
        decoded = json.loads(line)
        decoded = _dict_key_flatten(decoded)
        decoded_str = str(decoded)
        database.insert_event_data(conn, c, decoded)
#        print("Line %s" % lineNum)
        lineNum = lineNum + 1
        logging.basicConfig(filename='example.log', filemode='w',level=logging.DEBUG)
        #logging.debug('This message should go to the log file')
        #logging.info('So should this')
        #logging.warning('And this, too')
        if ( lineNum % 1000 == 0):
            logging.warning('Processing event %s', lineNum)
print("Done adding events to database!\n")
c.execute('SELECT timestamp FROM eventDATA')
timeAll = c.fetchall()
hours = {}
for time in timeAll:
   hour = datetime.datetime.fromtimestamp(time[0]/1000).strftime("%H")
   if hour in hours:
       hours[hour] += 1
   else:
       hours[hour] = 1
print(hours)

c.execute('SELECT COUNT(*) FROM appData')
appAmount = c.fetchone()[0]
appNumber = input("There is %s applications in database.\nWhich one launches count you want to get?\n" % appAmount)
str = 'launch'
c.execute('SELECT name FROM appData WHERE appId=?', [appNumber])
name = c.fetchone()[0]
c.execute('SELECT appId FROM eventData WHERE (eventType=? AND appID=?)', [str, appNumber])
apps = c.fetchall()
print("Application %s has been launched %s times" % (name, len(apps)))