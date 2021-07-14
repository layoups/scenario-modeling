from env import DB_CONN_PARAMETER
import numpy as np

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

from math import radians, cos, sin, asin, sqrt

def haversine_distance(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon/2) ** 2
    c = 2 * asin(sqrt(a)) 
    r = 6372.8 
    return c * r


if __name__ == '__main__':

    lon1 = 84.3880
    lat1 = 33.7490
    lon2 = 2.3522
    lat2 = 48.8566

    print(haversine_distance(lon1, lat1, lon2, lat2))
