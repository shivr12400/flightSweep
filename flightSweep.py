import json
import boto3
import urllib3
import math
import time
import os
from decimal import Decimal
from datetime import datetime, timezone

# --- CONFIGURATION ---
API_ENDPOINT = "https://5ny2oufcs1.execute-api.us-east-1.amazonaws.com/production"
# Fallback location (Woodbridge, NJ) if user hasn't sent GPS yet
DEFAULT_LAT, DEFAULT_LON = 40.587787, -74.333724 
RADIUS_NM = 5
AE_KEY = "5938ea-d28797"

dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table('FlightRadarConnections')
cache_table = dynamodb.Table('FlightRouteCache') 

apigw = boto3.client('apigatewaymanagementapi', endpoint_url=API_ENDPOINT)
http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=5.0, read=5.0))

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8 
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = math.sin((lat2 - lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_ae_timetable_data(callsign):
    """Fetches flight details from Timetable API with Scheduled fallback."""
    if not AE_KEY: return None
    url = f"https://aviation-edge.com/v2/public/timetable?key={AE_KEY}&flight_icao={callsign}"
    try:
        resp = http.request('GET', url)
        data = json.loads(resp.data.decode('utf-8'))
        
        if isinstance(data, list) and len(data) > 0:
            f = data[0]
            dep = f.get('departure', {})
            arr = f.get('arrival', {})
            
            # Logic: Prefer Estimated, Fallback to Scheduled
            est_dep = dep.get('estimatedTime') or dep.get('scheduledTime')
            est_arr = arr.get('estimatedTime') or arr.get('scheduledTime')
            ac_type = f.get('aircraft', {}).get('icaoCode', 'UNK')

            return {
                "origin": dep.get('iataCode', 'UNK'),
                "dest": arr.get('iataCode', 'UNK'),
                "est_dep": est_dep,
                "est_arr": est_arr,
                "type": ac_type
            }
    except Exception as e:
        print(f"AE Timetable Error for {callsign}: {e}")
    return None

def get_route_info(callsign):
    if not callsign or callsign == 'N/A': 
        return "UNK", "UNK", None, None, "UNK"
    
    # 1. Check Cache
    try:
        res = cache_table.get_item(Key={'callsign': callsign})
        if 'Item' in res:
            i = res['Item']
            return i['origin'], i['dest'], i.get('est_dep'), i.get('est_arr'), i.get('type', 'UNK')
    except: pass

    # 2. Fetch Fresh Data
    data = get_ae_timetable_data(callsign)
    if data:
        # Cache for 1 hour (TTL logic can be added to DynamoDB)
        cache_table.put_item(Item={
            'callsign': callsign, 'origin': data['origin'], 'dest': data['dest'], 
            'est_dep': data['est_dep'], 'est_arr': data['est_arr'], 'type': data['type']
        })
        return data['origin'], data['dest'], data['est_dep'], data['est_arr'], data['type']
    
    return "UNK", "UNK", None, None, "UNK"

def fetch_and_broadcast_for_user(connection_id, user_lat, user_lon):
    """Fetches ADSB data for a specific user location and sends it."""
    adsb_url = f"https://api.adsb.lol/v2/point/{round(user_lat,4)}/{round(user_lon,4)}/{RADIUS_NM}"
    
    try:
        resp = http.request('GET', adsb_url)
        data = json.loads(resp.data.decode('utf-8'))
        
        best_plane = None
        
        if data.get('ac'):
            planes = []
            for p in data['ac']:
                # Parse plane data
                lat, lon = float(p.get('lat', 0)), float(p.get('lon', 0))
                dist = haversine(user_lat, user_lon, lat, lon)
                
                # Filter strictly by distance (ADSB API box is square, this makes it circular)
                if dist <= RADIUS_NM:
                    callsign = str(p.get('flight', 'N/A')).strip()
                    
                    # Fetch extra details (Route, Aircraft Type, Times)
                    origin, dest, dep, arr, ac_type = get_route_info(callsign)
                    
                    # Time Left Calc (Backend Side)
                    time_left = None
                    if arr:
                        try:
                            ts_clean = arr.split('.')[0].replace('T', ' ')
                            arr_dt = datetime.strptime(ts_clean, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                            diff = arr_dt - datetime.now(timezone.utc)
                            time_left = max(0, int(diff.total_seconds() / 60))
                        except: pass

                    planes.append({
                        'callsign': callsign, 'lat': lat, 'lon': lon,
                        'alt': int(float(p.get('alt_baro', 0)) if p.get('alt_baro') != 'ground' else 0),
                        'speed': int(float(p.get('gs', 0))), 'heading': float(p.get('track', 0)),
                        'dist': round(dist, 2), 
                        'origin': origin, 'dest': dest,
                        'est_dep': dep, 'est_arr': arr, 
                        'time_left': time_left, 
                        'type': ac_type
                    })
            
            # Pick the closest one
            if planes:
                planes.sort(key=lambda x: x['dist'])
                best_plane = planes[0]

        # Send Update (or Empty if no planes)
        if best_plane:
            payload = json.dumps({'type': 'radar_update', 'flight': best_plane})
            try: 
                apigw.post_to_connection(ConnectionId=connection_id, Data=payload)
            except apigw.exceptions.GoneException:
                connections_table.delete_item(Key={'connectionId': connection_id})
            except Exception as e:
                print(f"Send Error {connection_id}: {e}")
                
    except Exception as e:
        print(f"ADSB Fetch Error: {e}")

def lambda_handler(event, context):
    """
    DUAL MODE HANDLER:
    1. If triggered by WebSocket 'update_location', it saves coordinates.
    2. If triggered by EventBridge (Schedule), it runs the broadcast loop.
    """
    
    # --- MODE 1: Handle Location Update from UI ---
    # The UI sends: { "action": "update_location", "lat": 12.34, "lon": 56.78 }
    if event.get('body'):
        try:
            body = json.loads(event['body'])
            if body.get('action') == 'update_location':
                cid = event['requestContext']['connectionId']
                lat = str(body.get('lat', DEFAULT_LAT)) # Store as string to avoid Decimal issues
                lon = str(body.get('lon', DEFAULT_LON))
                
                print(f"Updating location for {cid}: {lat}, {lon}")
                connections_table.update_item(
                    Key={'connectionId': cid},
                    UpdateExpression="set lat=:l, lon=:n",
                    ExpressionAttributeValues={':l': lat, ':n': lon}
                )
                return {"statusCode": 200, "body": "Location Updated"}
        except Exception as e:
            print(f"Update Handler Error: {e}")
            return {"statusCode": 500}

    # --- MODE 2: Scheduled Broadcaster Loop ---
    stop = time.time() + 50
    while time.time() < stop:
        start = time.time()
        
        # 1. Get all active connections
        scan = connections_table.scan()
        conns = scan.get('Items', [])
        
        # 2. Iterate and process EACH user individually
        for c in conns:
            cid = c['connectionId']
            # Default to fallback if DB doesn't have coords yet
            u_lat = float(c.get('lat', DEFAULT_LAT))
            u_lon = float(c.get('lon', DEFAULT_LON))
            
            fetch_and_broadcast_for_user(cid, u_lat, u_lon)
            
        # 3. Dynamic Sleep (aim for every 10s)
        time.sleep(max(1.0, 10.0 - (time.time() - start)))
        
    return {"statusCode": 200}