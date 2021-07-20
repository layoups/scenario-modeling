from env import DB_CONN_PARAMETER
import numpy as np

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

import googlemaps

karim_api_key = 'AIzaSyACWWCaFlJje26Yapq2ifqURXhXR5PfhUs'


def get_main_pflow(scenario_id, baseline_id, pdct_fam, session):

    model_pflows = session.query(ScenarioLanes.pflow).filter(
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id,
        ScenarioLanes.parent_pflow == None , 
        ScenarioLanes.pdct_fam == pdct_fam, 
        ScenarioLanes.in_pflow == 1
        ).distinct()
    return model_pflows


def get_lat_long(session):
    gm = googlemaps.Client(key=karim_api_key)

    locations = session.query(Locations).filter(Locations.lat == None)

    for location in locations.all():
        name = location.name
        api = gm.geocode(name)

        try:
            location.lat = api[0]["geometry"]["location"]["lat"]
            location.long = api[0]["geometry"]["location"]["lng"]

        except:
            None
        
    session.commit()

def get_node_supply(scenario_id, baseline_id, pdct_fam, session):
    nodes = session.query(ScenarioNodes).filter(
        ScenarioNodes.scenario_id == scenario_id,
        ScenarioNodes.baseline_id == baseline_id,
        ScenarioNodes.pdct_fam == pdct_fam
        )

    for n in nodes.all():
        if n.role == 'Customer':
            total = session.query(func.sum(ScenarioLanes.total_weight).label('total')).filter(
                ScenarioLanes.scenario_id == n.scenario_id,
                ScenarioLanes.baseline_id == n.baseline_id,
                ScenarioLanes.desti_name == n.name,
                ScenarioLanes.desti_country == n.country, 
                ScenarioLanes.desti_region == n.region, 
                ScenarioLanes.desti_role == n.role,
                ScenarioLanes.pdct_fam == n.pdct_fam,
                ScenarioLanes.in_pflow == 1
                ).group_by(
                    ScenarioLanes.desti_name,
                    ScenarioLanes.desti_country,
                    ScenarioLanes.desti_region,
                    ScenarioLanes.desti_role
                ).first()
            n.supply = -total.total
        else:
            n.supply = 0
    
    session.commit()

def get_pflow_demand(scenario_id, baseline_id, pflow, pdct_fam, session):
    pflow_demand = session.query(
            func.sum(ScenarioNodes.supply).label('total_supply')).filter(
                ScenarioLanes.scenario_id == scenario_id,
                ScenarioLanes.baseline_id == baseline_id,
                ScenarioLanes.desti_role == ScenarioNodes.role,
                ScenarioLanes.desti_name == ScenarioNodes.name,
                ScenarioLanes.desti_country == ScenarioNodes.country,
                ScenarioLanes.desti_region == ScenarioNodes.region,
                ScenarioLanes.pdct_fam == pdct_fam
            ).filter(
                ScenarioLanes.desti_role == 'Customer',
                or_(ScenarioLanes.pflow == pflow, ScenarioLanes.parent_pflow == pflow)  
            )
    return pflow_demand.first()

def get_node_capacity(scenario_id, baseline_id, pdct_fam, session):
    nodes = session.query(ScenarioNodes).filter(
        ScenarioNodes.scenario_id == scenario_id,
        ScenarioNodes.baseline_id == baseline_id,
        ScenarioNodes.pdct_fam == pdct_fam
        )

    model_pflows = get_main_pflow(scenario_id, baseline_id, pdct_fam, session)

    for pflow in model_pflows.all():
        pflow_demand = - get_pflow_demand(scenario_id, baseline_id, pflow.pflow, pdct_fam, session).total_supply
        stmt = text("""
            update "ScenarioNodes" 
            set capacity = sub.cap
            from (
                select total_alpha * :demand / 0.8 as cap, ori_role, ori_name, ori_country, ori_region
                from "ScenarioLanes" join "ScenarioNodes"
                on ori_role = role and ori_name = name and ori_country = country and ori_region = region 
                and "ScenarioLanes".scenario_id = "ScenarioNodes".scenario_id and "ScenarioLanes".baseline_id = "ScenarioNodes".baseline_id
                where ori_role in ('PCBA', 'DF', 'GHUB', 'OSLC', 'DSLC')
                and pflow = :pflow
            ) as sub
            where role = sub.ori_role and name = sub.ori_name and sub.ori_country = country and sub.ori_region = region
            and scenario_id = :scenario_id and baseline_id = :baseline_id
        """).params(demand = pflow_demand, pflow = pflow.pflow, scenario_id = scenario_id, baseline_id = baseline_id)
        session.execute(stmt)

        stmt = text("""
            update "ScenarioNodes"
            set capacity = -1
            where role in ('Customer', 'Supplier')
        """)
        session.execute(stmt)
    session.commit()





if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER)
    Session = sessionmaker(bind=engine)
    session = Session()

    get_node_supply('4400ISR', session)
    get_node_capacity('4400ISR', session)

    # get_lat_long(session)