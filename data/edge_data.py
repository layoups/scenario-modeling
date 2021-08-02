from env import DB_CONN_PARAMETER
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

from datetime import datetime

from math import radians, cos, sin, asin, sqrt
## FIX


ocean_matrix = {
    ('apac', 'apac'): {'time': 2 * 24, 'distance': 30},
    ('apac', 'us'): {'time': 36 * 24, 'distance': 20695}, 
    ('us', 'apac'): {'time': 36 * 24, 'distance': 20695}, 
    ('apac', 'latm'): {'time': 22 * 24, 'distance': 12662}, 
    ('latm', 'apac'): {'time': 22 * 24, 'distance': 12662}, 
    ('apac', 'eum'): {'time': 31 * 24, 'distance': 17865}, 
    ('eum', 'apac'): {'time': 31 * 24, 'distance': 17865}, 
    ('us', 'us'): {'time': 15 * 24, 'distance': 8792}, 
    ('us', 'eum'): {'time': 17 * 24, 'distance': 9533}, 
    ('eum', 'us'): {'time': 17 * 24, 'distance': 9533}, 
    ('latm', 'latm'): {'time': 22 * 24, 'distance': 12662}
    }

co2e_matrix = {
    'Air': 0.00113,
    'Truck': 0.00012318,
    'Ocean': 0.00001614,
    'Rail': 0.00002556
}


def populate_scenario_edges(scenario_id, baseline_id, session):
    baseline = session.query(Baselines).filter(Baselines.baseline_id == baseline_id).first()

    stmt = text("""
            insert into scdsi_scenario_edges 
            (scenario_id, baseline_id , 
            ori_name, ori_region, 
            desti_name, desti_region, 
            transport_cost, total_weight, transport_mode, in_pflow)
            select distinct :scenario_id, :baseline_id, lower(ship_from_name), lower(ship_from_region_code),
            lower(ship_to_name), lower(ship_to_region_code), sum(total_amount_paid_usd) / sum(billed_weight),
            sum(billed_weight),
            case
                when transport_mode in ('PARCEL', 'AIR') THEN 'Air'
                when transport_mode in ('LTL', 'LCL', 'TL', 'DRAY', 'WHSE') THEN 'Truck'
                when transport_mode in ('OCEAN') THEN 'Ocean'
                when transport_mode in ('RAIL') THEN 'Rail'
                else transport_mode
            end as mode, 0
            from scdsi_cv_lane_rate_automation_pl
            where billed_weight != 0 
            and shipment_type not in ('OTHER', 'BROKERAGE')
            and transport_mode is not null
            and ship_date_pure_ship >= :start and ship_date_pure_ship <= :end
            and ship_from_name is not null
            -- and ship_from_country is not null
            and ship_from_region_code is not null
            -- and length(ship_from_name) > 3
            and ship_to_name is not null
            -- and ship_to_country is not null
            and ship_to_region_code is not null
            -- and length(ship_to_name) > 3
            group by lower(ship_from_name), lower(ship_from_region_code), 
            lower(ship_to_name), lower(ship_to_region_code), 
            mode
        """).params(
            scenario_id=scenario_id, 
            baseline_id=baseline_id,
            start = baseline.start,
            end = baseline.end)

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

def get_distances_time_co2e(scenario_id, baseline_id, session):
    locations = Locations.get_locations(session)

    edges = session.query(ScenarioEdges).filter(
        ScenarioEdges.distance == None,
        ScenarioEdges.scenario_id == scenario_id,
        ScenarioEdges.baseline_id == baseline_id
        )
    # start = datetime.now()
    for edge in edges.all():
        try:
            ori_location = locations[(edge.ori_name, edge.ori_region)]
            desti_location = locations[(edge.desti_name, edge.desti_region)]
            ori_lat, ori_long = ori_location['lat'], ori_location['long']
            desti_lat, desti_long = desti_location['lat'], desti_location['long']
        except:
            ori_lat, ori_long = None, None
            desti_lat, desti_long = None, None
        edge.distance = 0 if not ori_lat or not desti_lat else haversine_distance(ori_lat, ori_long, desti_lat, desti_long)
        edge.transport_time, edge.co2e = get_transport_time_and_co2(edge)
    # print(datetime.now() - start)

    session.commit()

def get_transport_time_and_co2(edge):

    distance = float(edge.distance)
    mode = edge.transport_mode

    if mode == 'Air':
        transport_time = distance / 870
        co2e = distance * co2e_matrix[mode]

    if mode == 'Truck':
        transport_time = distance / 65
        co2e = distance * co2e_matrix[mode]

    if mode == 'Ocean':
        transport_time = ocean_matrix[(edge.ori_region, edge.desti_region)]['time']
        co2e = ocean_matrix[(edge.ori_region, edge.desti_region)]['distance'] * co2e_matrix[mode]

    if mode == 'Rail':
        transport_time = distance / 113
        co2e = distance * co2e_matrix[mode]

    return transport_time, co2e

def set_in_pflow_for_scenario_edges(scenario_id, baseline_id, session):
    edges = session.query(
        ScenarioLanes.ori_name,
        # ScenarioLanes.ori_country,
        ScenarioLanes.ori_region,
        # ScenarioLanes.ori_role,
        ScenarioLanes.desti_name,
        # ScenarioLanes.desti_country,
        ScenarioLanes.desti_region
        # ScenarioLanes.desti_role
        ).filter(
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id).distinct().all()

    for edge in edges:
        scenario_edges = session.query(ScenarioEdges).filter(
            ScenarioEdges.ori_name == edge.ori_name,
            # ScenarioEdges.ori_country == edge.ori_country,
            ScenarioEdges.ori_region == edge.ori_region,
            ScenarioEdges.desti_name == edge.desti_name,
            # ScenarioEdges.desti_country == edge.desti_country,
            ScenarioEdges.desti_region == edge.desti_region,
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id
        ).all()

        for e in scenario_edges:
            # e.ori_role = edge.ori_role
            # e.desti_role = edge.desti_role
            e.in_pflow = e.in_pflow

    session.commit()
    


if __name__ == '__main__':
    
    engine = create_engine(DB_CONN_PARAMETER_CLOUD)
    Session = sessionmaker(bind=engine)
    session = Session()

    populate_scenario_edges(0, 1, session)
    get_distances_time_co2e(0, 1, session)

    session.commit()
