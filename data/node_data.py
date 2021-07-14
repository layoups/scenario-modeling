from env import DB_CONN_PARAMETER
import numpy as np

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from AutoMap import *

import googlemaps

karim_api_key = 'AIzaSyACWWCaFlJje26Yapq2ifqURXhXR5PfhUs'


def get_main_pflow(pdct_fam, session):

    model_pflows = session.query(Lanes.pflow).filter(
        Lanes.parent_pflow == None , 
        Lanes.pdct_fam == pdct_fam, 
        Lanes.in_pflow == 1
        ).distinct()
    return model_pflows


def get_lat_long(session):
    gm = googlemaps.Client(key=karim_api_key)

    locations = session.query(Locations)

    for location in locations.all():
        name = location.name
        api = gm.geocode(name)

        try:
            location.lat = api[0]["geometry"]["location"]["lat"]
            location.long = api[0]["geometry"]["location"]["lng"]

        except:
            None
        
    session.commit()

    # for i in range(0,len (X.Address.to_list())):
    #     print(X.iat[i,2])
    #     result=gm.geocode(X.iat[i,2])
    #     #print(result)
    #     try:
    #         lat=result[0]["geometry"]["location"]["lat"]
    #         long=result[0]["geometry"]["location"]["lng"]
    #         print(lat,long)
    #         X.iat[i,X.columns.get_loc('LAT')] =lat
    #         X.iat[i,X.columns.get_loc('LONG')] =long
    #     except:
    #         lat=None
    #         long=None


def get_node_supply(pdct_fam, session):
    nodes = session.query(Nodes).filter(Nodes.pdct_fam == pdct_fam)

    for n in nodes.all():
        if n.role == 'Customer':
            total = session.query(func.sum(Lanes.total_weight).label('total')).filter(
                Lanes.desti_name == n.name,
                Lanes.desti_country == n.country, 
                Lanes.desti_region == n.region, 
                Lanes.desti_role == n.role,
                Lanes.pdct_fam == n.pdct_fam,
                Lanes.in_pflow == 1
                ).group_by(
                    Lanes.desti_name,
                    Lanes.desti_country,
                    Lanes.desti_region,
                    Lanes.desti_role
                ).first()
            n.supply = -total.total
        else:
            n.supply = 0
    
    session.commit()

def get_pflow_demand(pflow, pdct_fam, session):
    pflow_demand = session.query(
            func.sum(Nodes.supply).label('total_supply')).filter(
            Lanes.desti_role == Nodes.role,
            Lanes.desti_name == Nodes.name,
            Lanes.desti_country == Nodes.country,
            Lanes.desti_region == Nodes.region
        ).filter(
            Lanes.desti_role == 'Customer',
            or_(Lanes.pflow == pflow, Lanes.parent_pflow == pflow)  
        )
    return pflow_demand.first()

def get_node_capacity(pdct_fam, session):
    nodes = session.query(Nodes).filter(Nodes.pdct_fam == pdct_fam)

    model_pflows = get_main_pflow(pdct_fam, session)

    for pflow in model_pflows.all():
        pflow_demand = - get_pflow_demand(pflow.pflow, pdct_fam, session).total_supply
        stmt = text("""
            update "Nodes" 
            set capacity = sub.cap
            from (
                select total_alpha * :demand as cap, ori_role, ori_name, ori_country, ori_region
                from "Lanes" join "Nodes"
                on ori_role = role and ori_name = name and ori_country = country and ori_region = region
                where ori_role in ('PCBA', 'DF', 'GHUB', 'OSLC', 'DSLC')
                and pflow = :pflow
            ) as sub
            where role = sub.ori_role and name = sub.ori_name and sub.ori_country = country and sub.ori_region = region
        """).params(demand = pflow_demand, pflow = pflow.pflow)
        session.execute(stmt)

        stmt = text("""
            update "Nodes"
            set capacity = -1
            where role in ('Customer', 'Supplier')
        """)
        session.execute(stmt)
    session.commit()





if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER)
    Session = sessionmaker(bind=engine)
    session = Session()

    # get_node_supply('4400ISR', session)
    # get_node_capacity('4400ISR', session)

    get_lat_long(session)