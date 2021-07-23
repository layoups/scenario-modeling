from env import DB_CONN_PARAMETER
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

from datetime import datetime

from math import radians, cos, sin, asin, sqrt

def populate_scenario_edges(scenario_id, baseline_id, session):
    stmt = text("""
            insert into "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" 
            ("SCENARIO_ID", "BASELINE_ID" , "ORI_NAME", "ORI_COUNTRY", "ORI_REGION", "DESTI_NAME", "DESTI_COUNTRY", "DESTI_REGION", "TRANSPORT_MODE")
            select distinct :scenario_id, :baseline_id, lower("SHIP_FROM_NAME"), lower("SHIP_FROM_COUNTRY"), lower("SHIP_FROM_REGION_CODE"),
            lower("SHIP_TO_NAME"), lower("SHIP_TO_COUNTRY"), lower("SHIP_TO_REGION_CODE"),
            case
                when "TRANSPORT_MODE" in ('PARCEL', 'AIR') THEN 'Air'
                when "TRANSPORT_MODE" in ('LTL', 'LCL', 'TL', 'DRAY', 'WHSE') THEN 'Truck'
                when "TRANSPORT_MODE" in ('OCEAN') THEN 'Ocean'
                when "TRANSPORT_MODE" in ('RAIL') THEN 'Rail'
                else "TRANSPORT_MODE"
            end
            from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_CV_LANE_RATE_AUTOMATION_PL" 
            where "BILLED_WEIGHT" != 0 
            and "SHIPMENT_TYPE" not in ('OTHER', 'BROKERAGE')
            and "TRANSPORT_MODE" is not null
        """).params(scenario_id=scenario_id, baseline_id=baseline_id)

    session.execute(stmt)
    session.commit()

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

def get_distances(session):
    locations = get_locations(session)

    edges = session.query(Edges).filter(Edges.transport_distance == None)
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

    get_distances(session)

    session.commit()
