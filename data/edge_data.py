from env import DB_CONN_PARAMETER
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

from datetime import datetime

from math import radians, cos, sin, asin, sqrt

def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1 
    dlon = lon2 - lon1 

    a = sin(dlat/2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon/2) ** 2
    c = 2 * asin(sqrt(a)) 
    r = 6372.8 
    return c * r

def get_locations(session):
    locations = session.query(Locations).all()
    return {(x.name, x.country, x.region): {'lat': x.lat, 'long': x.long} for x in locations}

def insert_lat_long(session):
    locations = get_locations(session)

    edges = session.query(Edges)
    # start = datetime.now()
    for edge in edges.all():
        ori_location = locations[(edge.ori_name, edge.ori_country, edge.ori_region)]
        desti_location = locations[(edge.desti_name, edge.desti_country, edge.desti_region)]
        ori_lat, ori_long = ori_location['lat'], ori_location['long']
        desti_lat, desti_long = desti_location['lat'], desti_location['long']
        edge.transport_distance = 0 if not ori_lat or not desti_lat else haversine_distance(ori_lat, ori_long, desti_lat, desti_long)
    # print(datetime.now() - start)

    session.commit()

if __name__ == '__main__':
    
    engine = create_engine(DB_CONN_PARAMETER)
    Session = sessionmaker(bind=engine)
    session = Session()

    lon1 = 84.3880
    lat1 = 33.7490
    lon2 = 2.3522
    lat2 = 48.8566

    insert_lat_long(session)
