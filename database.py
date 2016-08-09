import sqlite3
import logging
import data as dt

'''
 Function for creating database if it doesn't exist,
 opening connection to it and creating needed tables
'''
def initializeDatabase(conn, c):
    # Create main table to hold all pointers to basic event information
    # With deviceID and timestamp being key
    c.execute('''CREATE TABLE IF NOT EXISTS eventData (
        eventID       TEXT,
        deviceID      INTEGER,
        timestamp     INTEGER,
        eventType     TEXT,
        appID         INTEGER,
        geoID         INTEGER,
        system        TEXT,
        interactionID INTEGER,
        networkID     INTEGER,
        addInfoID     INTEGER
    )''')

    # Create table to hold information about application
    c.execute('''CREATE TABLE IF NOT EXISTS appData (
        appID         INTEGER PRIMARY KEY,
        name          TEXT,
        version       TEXT,
        language      TEXT,
        package       TEXT
    )''')

    # Create table to hold information about device
    c.execute('''CREATE TABLE IF NOT EXISTS deviceData (
        deviceID      TEXT,
        deviceType    TEXT,
        manufacturer  TEXT,
        model         TEXT,
        memory        INTEGER,
        architecture  TEXT,
        language      TEXT,
        systemID      INTEGER,
        res_height    INTEGER,
        res_width     INTEGER
    )''')

    # Create table to hold information about operating system
    c.execute('''CREATE TABLE IF NOT EXISTS systemData (
        systemID      INTEGER PRIMARY KEY,
        kind          TEXT,
        version       TEXT,
        name          TEXT
    )''')

    # Create table to hold information about network
    c.execute('''CREATE TABLE IF NOT EXISTS networkData (
        networkID     INTEGER PRIMARY KEY,
        carrier       TEXT,
        connection    TEXT
    )''')

    # Create table to hold information about geo localization
    c.execute('''CREATE TABLE IF NOT EXISTS geoData (
        geoID         INTEGER PRIMARY KEY,
        city          TEXT,
        country       TEXT,
        region        TEXT,
        range_X       INTEGER,
        range_Y       INTEGER,
        latitude      INTEGER,
        longitude     INTEGER
    )''')

    # Create table to hold information about user interaction
    c.execute('''CREATE TABLE IF NOT EXISTS interactionData (
        interactiomID INTEGER PRIMARY KEY,
        type          TEXT,
        viewID        TEXT
    )''')

# Cache dictionaries
networkCache = {}
systemCache = {}
deviceCache = {}
eventCache = []
appCache = {}
geoCache = {}

'''
 Function inserting data into networkData table
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing network information
                  into database
 Returns:
           row number where data was entered or found
'''
def insert_network(conn, c, data):
    network = dt.get_networkData(data)

    networkStr = network['carrier'] + '_' + network['connection']
    if networkStr in networkCache:
        logging.debug("Network %s exists and is cached." % networkStr)
        return networkCache[networkStr]
    else:
        c.execute("SELECT rowid FROM networkData WHERE (carrier=? AND connection=?)",
                  [ network['carrier'],
                    network['connection'] ]
                 )
        row = c.fetchone()
        #Exists, return the row number
        if not (row == None):
            networkCache.update({networkStr:row[0]})
            logging.debug("Network %s exists and got cached." % networkStr)
            return row[0]
        # Does not exist, add it into database and return row number
        else:
            c.execute("INSERT INTO networkData VALUES (NULL, ?, ?)",
                       [ network['carrier'],
                         network['connection'] ]
                     )
            networkCache.update({networkStr:c.lastrowid})
            logging.debug("Inserted network: %s %s into database" % ( network['carrier'], network['connection'] ))
            return c.lastrowid

'''
 Function inserting data into systemData table
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing system information
                  into database
 Returns:
           row number where data was entered or found
'''
def insert_system(conn, c, data):
    system = dt.get_systemData(data)

    systemStr = system['kind'] + '_' + system['version'] + '_' + system['name']
    # If our system is in cache dictionary
    if systemStr in systemCache:
        logging.debug("System %s exists and is cached." % systemStr)
        return systemCache[systemStr]
    # We do not have system in cache dictionary
    else:
        # Check if our operating system exists in our database
        c.execute("SELECT rowid FROM systemData WHERE (kind=? AND version=? AND name=?)",
                  [ system['kind'],
                    system['version'],
                    system['name'] ]
                 )
        row = c.fetchone()
        # Exists, return the row number and update cache
        if not (row == None):
            systemCache.update({systemStr:row[0]})
            logging.debug("System %s exists and got cached." % systemStr)
            return row[0]
        # Does not exist, add it into database, add to cache and return row number
        else:
            c.execute("INSERT INTO systemData VALUES (NULL, ?, ?, ?)",
                      [ system['kind'],
                        system['version'],
                        system['name'] ]
                     )
            systemCache.update({systemStr:c.lastrowid})
            logging.debug("Inserted system: %s %s %s into database",
                           system['kind'],
                           system['version'],
                           system['name']
                         )
            return c.lastrowid

'''
 Function inserting data into appData table
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing application information
                  into database
 Returns:
           row number where data was entered or found
'''
def insert_application(conn, c, data):
    app = dt.get_appData(data)

    appStr = app['name'] + "_" + app['version']  + "_" + app['language'] + "_" + app['package_name']
    if appStr in appCache:
        logging.debug("Application %s exists and is cached." % appStr)
        return appCache[appStr]
    else:
        c.execute("SELECT rowid FROM appData WHERE (name=? AND version=? AND language=? AND package=?)",
                  [ app['name'],
                    app['version'],
                    app['language'],
                    app['package_name'] ]
                 )
        row = c.fetchone()
        if not(row == None):
            appCache.update({appStr:row[0]})
            logging.debug("Application %s exists and got cached." % appStr)
            return row[0]
        else:
            c.execute("INSERT INTO appData VALUES (NULL, ?, ?, ?, ?)",
                      [ app['name'],
                        app['version'],
                        app['language'],
                        app['package_name'] ]
                     )
            logging.debug("Inserted application: %s %s %s %s into database",
                           app['name'],
                           app['version'],
                           app['language'],
                           app['package_name']
                         )
            return c.lastrowid

'''
 Function inserting data into geoData table
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing geographical information
                  into database
 Returns:
           row number where data was entered or found
'''
def insert_geo(conn, c, data):
    geo = dt.get_geoData(data)
    
    if geo['hash'] in geoCache:
        logging.debug("Geo exists and is cached.")
        return geoCache[geo['hash']]
    else:
        # Check if our geo exists in our database
        c.execute("SELECT rowid FROM geoData WHERE (city=? AND country=? AND region=? AND range_X=? AND range_Y=? AND latitude=? AND longitude=?)",
                  [ geo['city'] ,
                    geo['country'],
                    geo['region'],
                    geo['range0'],
                    geo['range1'],
                    geo['latitude'],
                    geo['longitude'] ]
                 )
        row = c.fetchone()
        # Exists, return the row number
        if not (row == None):
            logging.debug("Found existing geo data: %s" % row)
            geoCache.update({geo['hash']:row[0]})
            return row[0]
        # Does not exist, add it into database and return row number
        else:
            c.execute("INSERT INTO geoData VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)",
                      [ geo['city'] ,
                        geo['country'],
                        geo['region'],
                        geo['range0'],
                        geo['range1'],
                        geo['latitude'],
                        geo['longitude'] ]
                     )
            geoCache.update({geo['hash']:c.lastrowid})
            return c.lastrowid

'''
 Function inserting device into deviceData table
 Calls functions for inserting system data
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing geographical information
                  into database
 Returns:
           row number where data was entered or found
'''
def insert_device(conn, c, data):
    device = dt.get_deviceData(data)

    if device['deviceId'] in deviceCache:
        logging.debug("Device %s exists and is cached." % device['deviceId'])
        return deviceCache[device['deviceId']]
    else:
        c.execute("SELECT rowid FROM deviceData WHERE deviceId=?", [device['deviceId']])
        row = c.fetchone()
        # Exists, return the row number
        if not (row == None):
            logging.debug("Device %s exists and got cached!" % device['deviceId'])
            deviceCache.update({device['deviceId']:row[0]})
            return row[0]
        # Does not exist, add it into database and return row number
        else:
            c.execute("INSERT INTO deviceData VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      [ device['deviceId'],
                        device['type'],
                        device['manufacturer'],
                        device['model'],
                        device['memory'],
                        device['architecture'],
                        device['lang'],
                        insert_system(conn, c, data),
                        device['height'],
                        device['width'] ]
                     )
            deviceCache.update({device['deviceId']:c.lastrowid})
            logging.debug("Adding device %s" % device['deviceId'])
            return c.lastrowid

'''
 Function inserting event into eventData table
 Calls functions for inserting device, application,
 geo and network information
 Arguments:
           conn - connection to database
           c    - database coursor
           data - data containing geographical information
                  into database
'''
def insert_event_data(conn, c, data):
    event = dt.get_eventData(data)

    if event['eventId'] in eventCache:
        logging.warning("Duplicate event %s", event['eventId'])
        logging.debug("Event %s exists and is cached." % event['eventId'])
    else:
        c.execute("SELECT rowid FROM eventData WHERE eventID=?", [event['eventId']])
        row = c.fetchone()
        if not (row == None):
            logging.warning("Duplicate event %s", event['eventId'])
            eventCache.append(event['eventId'])
        # Does not exist, add it into database and return row number
        else:
            c.execute("INSERT INTO eventData VALUES ( ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)",
                      [ event['eventId'],
                        insert_device(conn, c, data),
                        event['timestamp'],
                        event['type'],
                        insert_application(conn, c, data),
                        insert_geo(conn, c, data),
                        event['system'],
                        #insert_interaction_data,
                        insert_network(conn, c, data)
                        #insert_additional_info
                      ]
                     )
            eventCache.append(event['eventId'])
            logging.debug("Adding event %s" % event['eventId'])
