import hashlib

def get_systemData(data):
    system = {}
    if 'device.operating_system.name' in data:
        system.update({'name':data['device.operating_system.name'].encode('ascii','ignore').decode('utf-8')})
    else:
        system.update({'name':'unknown'})
    if 'device.operating_system.kind' in data:
        system.update({'kind':data['device.operating_system.kind'].encode('ascii','ignore').decode('utf-8')})
    else:
        system.update({'kind':'unknown'})
    if 'device.operating_system.version' in data:
        system.update({'version':data['device.operating_system.version'].encode('ascii','ignore').decode('utf-8')})
    else:
        system.update({'version':'unknown'})
    return system

def get_appData(data):
    app = {}
    if 'application.name' in data:
        app.update({'name':data['application.name'].encode('ascii','ignore').decode('utf-8')})
    else:
        app.update({'name':'unknown'})
    if 'application.package_name' in data:
        app.update({'package_name':data['application.package_name'].encode('ascii','ignore').decode('utf-8')})
    else:
        app.update({'package_name':'unknown'})
    if 'application.language' in data:
        app.update({'language':data['application.language'].encode('ascii','ignore').decode('utf-8')})
    else:
        app.update({'language':'unknown'})
    if 'application.version' in data:
        app.update({'version':data['application.version'].encode('ascii','ignore').decode('utf-8')})
    else:
        app.update({'version':'unknown'})
    return app

def get_networkData(data):
    network = {}
    if 'network.carrier' in data:
        network.update({'carrier':data['network.carrier'].encode('ascii','ignore').decode('utf-8')})
    else:
        network.update({'carrier':'unknown'});
    if 'network.connection' in data:
        network.update({'connection':data['network.connection'].encode('ascii','ignore').decode('utf-8')})
    else:
        network.update({'connection':'unknown'});
    return network

def get_geoData(data):
    geo = {}
    if 'sender_info.geo.region' in data:
        geo.update({'region':data['sender_info.geo.region'].encode('ascii','ignore').decode('utf-8')})
    else:
        geo.update({'region':'unknown'})
    if 'sender_info.geo.city' in data:
        geo.update({'city':data['sender_info.geo.city'].encode('ascii','ignore').decode('utf-8')})
    else:
        geo.update({'city':'unknown'})
    if 'sender_info.geo.country' in data:
        geo.update({'country':data['sender_info.geo.country'].encode('ascii','ignore').decode('utf-8')})
    else:
        geo.update({'country':'unknown'})
    if 'sender_info.geo.range0' in data:
        geo.update({'range0':data['sender_info.geo.range0']})
    else:
        geo.update({'range0':-1})
    if 'sender_info.geo.range1' in data:
        geo.update({'range1':data['sender_info.geo.range1']})
    else:
        geo.update({'range1':-1})
    if 'sender_info.geo.ll0' in data:
        geo.update({'latitude':data['sender_info.geo.ll0']})
    else:
        geo.update({'latitude':-1})
    if 'sender_info.geo.ll1' in data:
        geo.update({'longitude':data['sender_info.geo.ll1']})
    else:
        geo.update({'longitude':-1})
    
    #Generate hash of geo localisation for cache dictionary
    geoStr = geo['region'] + "_" + geo['city']+ "_" + geo['country'] + "_" + str(geo['range0']) + "_" + str(geo['range1']) + "_" + str(geo['latitude'])+ "_" + str(geo['longitude'])
    geo.update({'hash':hashlib.md5(geoStr.encode()).hexdigest()})

    return geo

def get_deviceData(data):
    device = {}
    if 'device.device_id' in data:
        device.update({'deviceId':data['device.device_id'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'deviceId':'unknown'})
    if 'device.device_type' in data:
        device.update({'type':data['device.device_type'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'type':'unknown'})
    if 'device.manufacturer' in data:
        device.update({'manufacturer':data['device.manufacturer'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'manufacturer':'unknown'})
    if 'device.model' in data:
        device.update({'model':data['device.model'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'model':'unknown'})
    if 'device.memory' in data:
        device.update({'memory':data['device.memory']})
    else:
        device.update({'memory':-1})
    if 'device.architecture' in data:
        device.update({'architecture':data['device.architecture'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'architecture':'unknown'})
    if 'device.language' in data:
        device.update({'lang':data['device.language'].encode('ascii','ignore').decode('utf-8')})
    else:
        device.update({'lang':'unknown'})
    if 'device.display_resolution.width' in data:
        device.update({'width':data['device.display_resolution.width']})
    else:
        device.update({'width':-1})
    if 'device.display_resolution.height' in data:
        device.update({'height':data['device.display_resolution.height']})
    else:
        device.update({'height':-1})
    return device

def get_eventData(data):
    event = {}
    event.update({'deviceId':data['device.device_id'].encode('ascii','ignore').decode('utf-8')})
    if 'timestamp' in data:
        event.update({'timestamp':data['timestamp']})
    elif 'time.create_timestamp' in data:
        event.update({'timestamp':data['time.create_timestamp']})
    else:
        event.update({'timestamp':0})
    if 'type' in data:
        event.update({'type':data['type'].encode('ascii','ignore').decode('utf-8')})
    else:
        event.update({'type':'unknown'})
    if 'system.deployment_name' in data:
        event.update({'system':data['system.deployment_name'].encode('ascii','ignore').decode('utf-8')})
    else:
        event.update({'system':'unknown'})
    if 'event_id' in data:
        event.update({'eventId':data['event_id'].encode('ascii','ignore').decode('utf-8')})
    else:
        event.update({'eventId':(event['deviceId'] + '_' + str(event['timestamp']))})
    return event