from networkx.classes.function import edges, nodes
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker

from collections import deque

from datetime import datetime

from sqlalchemy.sql.expression import or_

from AutoMap import *
from PathRoles import *
from Eraser import *
from Visualize import *
from Alpha import *
from dfs import *


def create_scenario(baseline_id, descriprion, session):
    scenario = Scenarios(baseline_id=baseline_id)
    scenario.date = datetime.now()
    scenario.description = descriprion

    session.add(scenario)
    session.commit()

    # copy scenario lanes, scenario edges, scenario nodes
    

# node_dict = {
#     'pdct_fam': , 
#     'name': , 
#     'country': , 
#     'region': , 
#     'role': , 
#     'capacity': , 
#     'supply': , 
#     'opex': ,
#     'alt_name': ,
#     'alt_country': ,
#     'alt_region': 
# } 
# add to alt nodes
# add to scenario nodes
# add to locations, get Locations
# add to alternative edges
# add to scenario edges, get distance, co2e, time
def add_alt_nodes(scenario_id, baseline_id, node_dict, session):
    if not session.query(Locations).filter(Locations.name == node_dict['name']):
        location = Locations(
            name = node_dict['name'],
            country = node_dict['country'],
            region = node_dict['region'],
        )
        session.add(location)
        session.commit()

    alt_node = AltNodes(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        country = node_dict['country'],
        region = node_dict['region'],
        alt_name = node_dict['alt_name'],
        alt_country = node_dict['alt_country'],
        alt_region = node_dict['alt_region'],
        capacity = node_dict['capacity'],
        supply = node_dict['supply'],
        opex = node_dict['opex']
    )
    session.add(alt_node)
    session.commit()

    scenario_node = ScenarioNodes(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        country = node_dict['country'],
        region = node_dict['region'],
        capacity = node_dict['capacity'],
        supply = node_dict['supply'],
        opex = node_dict['opex']
    )
    session.add(scenario_node)
    session.commit()

    stmt = text(
        """
        insert into scdsi_alternative_edges 
            (scenario_id, baseline_id, pdct_fam, 
            ori_name, ori_country, ori_region, ori_role 
            desti_name, desti_country, desti_region, desti_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow)
        select scenario_id, baseline_id, pdct_fam,
            :alt_name, :alt_country, :alt_region, :alt_role,
            desti_name, desti_country, desti_region, desti_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow 
        from scdsi_scenario_lanes
            where scenario_id in (:scenario_id)
                and baseline_id in (:baseline_id)
                and pdct_fam in (:pdct_fam)
                and ori_name in (:ori_name)
                and ori_country in (:ori_country)
                and ori_region in (:ori_region)
                and ori_role in (:ori_role)
        """
    ).params(
        alt_name = alt_node.alt_name,
        alt_country = alt_node.alt_country,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        scenario_id = scenario_id,
        baseline_id = baseline_id,
        pdct_fam = pdct_fam,
        ori_name = alt_node.name,
        ori_country = alt_node.country,
        ori_region = alt_node.region,
        ori_role = alt_node.role
    )

    session.execute(stmt)
    session.commit()

    stmt = text(
        """
        insert into scdsi_alternative_edges 
            (scenario_id, baseline_id, pdct_fam, 
            ori_name, ori_country, ori_region, ori_role 
            desti_name, desti_country, desti_region, desti_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow)
        select scenario_id, baseline_id, pdct_fam,
            ori_name, ori_country, ori_region, ori_role,
            :alt_name, :alt_country, :alt_region, :alt_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow 
        from scdsi_scenario_lanes
            where scenario_id in (:scenario_id)
                and baseline_id in (:baseline_id)
                and pdct_fam in (:pdct_fam)
                and desti_name in (:desti_name)
                and desti_country in (:desti_country)
                and desti_region in (:desti_region)
                and desti_role in (:desti_role)
        """
    ).params(
        alt_name = alt_node.alt_name,
        alt_country = alt_node.alt_country,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        scenario_id = scenario_id,
        baseline_id = baseline_id,
        pdct_fam = pdct_fam,
        desti_name = alt_node.name,
        desti_country = alt_node.country,
        desti_region = alt_node.region,
        desti_role = alt_node.role
    )

    session.execute(stmt)
    session.commit()


# node_dict = {'pdct_fam': , 'name': , 'country': , 'region': , 'role': }
def add_decom_nodes(scenario_id, baseline_id, node_dict, session):
    decom_node = DecomNodes(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        country = node_dict['country'],
        region = node_dict['region']
    )
    session.add(decom_node)
    session.commit()
    
    stmt = text(
        """
        update scdsi_scenario_nodes 
        set in_pflow = 0
        where scenario_id in (:scenario_id)
            and baseline_id in (:baseline_id)
            and pdct_fam in (:pdct_fam)
            and name in (:name)
            and country in (:country)
            and region in (:region)
            and role in (:role)
        """
    ).params(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        country = node_dict['country'],
        region = node_dict['region']
    )

    session.execute(stmt)
    session.commit()

    stmt = text(
        """
        update scdsi_decommisioned_edges
        set in_pflow = 0
        where scenario_id in (:scenario_id)
            and baseline_id in (:baseline_id)
            and pdct_fam in (:pdct_fam)
            and (ori_name in (:name) or desti_name in (:name))
            and (ori_country in (:country) or desti_country in (:country))
            and (ori_region in (:region) or desti_region in (:region))
            and (ori_role in (:role) or desti_role in (:role))
        """
    ).params(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        country = node_dict['country'],
        region = node_dict['region']
    )

    session.execute(stmt)
    session.commit()


# edge_dict = {
#     'pdct_fam': ,
#     'ori_name': ,
#     'ori_country': ,
#     'ori_region': ,
#     'ori_role': ,
#     'desti_name': ,
#     'desti_country': ,
#     'desti_region': ,
#     'desti_role': 
# }
def add_alt_edges(scenario_id, baseline_id, edge_dict, session):
    return None

# edge_dict = {
#     'pdct_fam': ,
#     'ori_name': ,
#     'ori_country': ,
#     'ori_region': ,
#     'ori_role': ,
#     'desti_name': ,
#     'desti_country': ,
#     'desti_region': ,
#     'desti_role': 
# }
def add_decom_edges(scenario_id, baseline_id, edge_dict, session):
    return None


def update_scenario_edges(scenario_id, baseline_id, session):
    return None


def update_scenario_lanes(scenario_id, baseline_id, session):
    alt_edges = session.query(AltEdges).all()
    for e in alt_edges:
        lane = ScenarioLanes(
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            pdct_fam = e.pdct_fam,
            ori_name = e.ori_name,
            # ori_country = Column('ori_country', String)
            # ori_region = Column('ori_region', String)
            # ori_role = Column('ori_role', String, default='')
            # desti_name = Column('desti_name', String)
            # desti_country = Column('desti_country', String)
            # desti_region = Column('desti_region', String)
            # desti_role = Column('desti_role', String, default='')
            # ship_type = Column('ship_type', String)
            # ship_rank = Column('ship_rank', Integer)
            # total_weight = Column('total_weight', Float)
            # total_paid = Column('total_paid', Float)
            # alpha = Column('alpha', Float)
            # total_alpha = Column('total_alpha', Float)
            # color = Column('color', Integer, default=0)
            # d = Column('d', Integer, default=0)
            # f = Column('f', Integer, default=0)
            # path = Column('path', Integer, default=0)
            # path_rank = Column('path_rank', Integer, default=0)
            # pflow = Column('pflow', Integer, default=0)
            # parent_pflow = Column('parent_pflow', Integer)
            # in_pflow = Column('in_pflow', Integer)
        )
    return None

if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER_WI)
    Session = sessionmaker(bind=engine)
    session = Session()

    location = session.query(Locations).first()
    location2 = location
    # location2.name = 'hi'
    print(location.name)

    session.close()