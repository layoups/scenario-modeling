from sqlalchemy.sql.coercions import expect
from node_data import get_lat_long
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


def create_scenario(scenario_id, baseline_id, description, session):
    scenario = Scenarios(
        scenario_id = scenario_id,
        baseline_id = baseline_id,
        date = datetime.now(),
        description = description
    )

    session.add(scenario)
    session.commit()

    

    stmt = text(
        """
        insert into scdsi_scenario_lanes
        (scenario_id, baseline_id, pdct_fam, 
        ori_name, ori_region, ori_role,
        desti_name, desti_region, desti_role,
        ship_type, ship_rank,
        total_weight, total_paid,
        color, d, f, alpha, total_alpha, path, path_rank,
        pflow, parent_pflow, in_pflow)
        select :scenario_id, baseline_id, pdct_fam, 
        ori_name, ori_region, ori_role,
        desti_name, desti_region, desti_role,
        ship_type, ship_rank,
        total_weight, total_paid,
        color, d, f, alpha, total_alpha, path, path_rank,
        pflow, parent_pflow, in_pflow
        from scdsi_scenario_lanes
        where baseline_id = :baseline_id
        and scenario_id = 0
        """
    ).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    session.execute(stmt)
    session.commit()

    stmt = text(
        """
        insert into scdsi_scenario_nodes
        (scenario_id, baseline_id, pdct_fam, 
        name, region, role,
        supply, capacity, opex,
        total_alpha, pflow, in_pflow)
        select :scenario_id, baseline_id, pdct_fam,
        name, region, role,
        supply, capacity, opex,
        total_alpha, pflow, in_pflow
        from scdsi_scenario_nodes
        where baseline_id = :baseline_id
        and scenario_id = 0
        """
    ).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    session.execute(stmt)
    session.commit()

    stmt = text(
        """
        insert into scdsi_scenario_edges
        (scenario_id, baseline_id, 
        ori_name, ori_region,
        desti_name, desti_region,
        transport_mode, distance, transport_time,
        co2e, transport_cost, total_weight, in_pflow)
        select :scenario_id, baseline_id, 
        ori_name, ori_region,
        desti_name, desti_region,
        transport_mode, distance, transport_time,
        co2e, transport_cost, total_weight, in_pflow
        from scdsi_scenario_edges
        where baseline_id = :baseline_id
        and scenario_id = 0
        """
    ).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    session.execute(stmt)
    session.commit()
    return True

    # copy scenario lanes, scenario edges, scenario nodes
    

node_dict = {
    'pdct_fam': '4400ISR' , 
    'name': ',cn',  
    'region': 'apac', 
    'role': 'PCBA', 
    'capacity': 8431, 
    'supply': 0, 
    'opex': 0,
    'alt_name': 'hanoi,vn',
    'alt_region': 'apac',
} 
# add to alt nodes
# add to scenario nodes
# add to locations, get Locations
# add to alternative edges
# add to scenario edges, get distance, co2e, time
def add_alt_nodes(scenario_id, baseline_id, node_dict, session):
    if not session.query(Locations).filter(Locations.name == node_dict['name']).first():
        location = Locations(
            name = node_dict['alt_name'],
            region = node_dict['alt_region'],
        )
        session.add(location)
        session.commit()
        get_lat_long(session, location)

    alt_node = AltNodes(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        region = node_dict['region'],
        alt_name = node_dict['alt_name'],
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
        name = node_dict['alt_name'],
        region = node_dict['alt_region'],
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
            ori_name, ori_region, ori_role, 
            desti_name, desti_region, desti_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow)
        select scenario_id, baseline_id, pdct_fam,
            :alt_name, :alt_region, :alt_role,
            desti_name, desti_region, desti_role, 
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
                and ori_region in (:ori_region)
                and ori_role in (:ori_role)
        """
    ).params(
        alt_name = alt_node.alt_name,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        scenario_id = scenario_id,
        baseline_id = baseline_id,
        pdct_fam = alt_node.pdct_fam,
        ori_name = alt_node.name,
        ori_region = alt_node.region,
        ori_role = alt_node.role
    )

    session.execute(stmt)
    session.commit()

    stmt = text(
        """
        insert into scdsi_alternative_edges 
            (scenario_id, baseline_id, pdct_fam, 
            ori_name, ori_region, ori_role, 
            desti_name, desti_region, desti_role, 
            ship_type, ship_rank, 
            total_weight, total_paid, 
            alpha, total_alpha,
            color, d, f,
            path, path_rank, pflow, parent_pflow, in_pflow)
        select scenario_id, baseline_id, pdct_fam,
            ori_name, ori_region, ori_role,
            :alt_name, :alt_region, :alt_role, 
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
                and desti_region in (:desti_region)
                and desti_role in (:desti_role)
        """
    ).params(
        alt_name = alt_node.alt_name,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        scenario_id = scenario_id,
        baseline_id = baseline_id,
        pdct_fam = alt_node.pdct_fam,
        desti_name = alt_node.name,
        desti_region = alt_node.region,
        desti_role = alt_node.role
    )

    session.execute(stmt)
    session.commit()


# node_dict = {'pdct_fam': , 'name': , 'region': , 'role': }
def add_decom_nodes(scenario_id, baseline_id, node_dict, session):
    decom_node = DecomNodes(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
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
            and region in (:region)
            and role in (:role)
        """
    ).params(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
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
            and (ori_region in (:region) or desti_region in (:region))
            and (ori_role in (:role) or desti_role in (:role))
        """
    ).params(
        scenario_id=scenario_id, 
        baseline_id=baseline_id,
        pdct_fam = node_dict['pdct_fam'],
        role = node_dict['role'],
        name = node_dict['name'],
        region = node_dict['region']
    )

    session.execute(stmt)
    session.commit()


# edge_dict = {
#     'pdct_fam': ,
#     'ori_name': ,
#     'ori_region': ,
#     'ori_role': ,
#     'desti_name': ,
#     'desti_region': ,
#     'desti_role': 
# }
def add_alt_edges(scenario_id, baseline_id, edge_dict, session):
    return None

# edge_dict = {
#     'pdct_fam': ,
#     'ori_name': ,
#     'ori_region': ,
#     'ori_role': ,
#     'desti_name': ,
#     'desti_region': ,
#     'desti_role': 
# }
def add_decom_edges(scenario_id, baseline_id, edge_dict, session):
    return None


def update_scenario_edges(scenario_id, baseline_id, session):
    alt_edges = session.query(AltEdges).filter(
        AltEdges.scenario_id == scenario_id,
        AltEdges.baseline_id == baseline_id
    ).all()
    for e in alt_edges:
        if not session.query(ScenarioEdges).filter(
            ScenarioEdges.ori_name == e.ori_name,
            ScenarioEdges.ori_region == e.ori_region,
            ScenarioEdges.desti_name == e.desti_name,
            ScenarioEdges.desti_region == e.desti_region,
            ):

    return None


def update_scenario_lanes(scenario_id, baseline_id, session):
    alt_edges = session.query(AltEdges).all()
    for e in alt_edges:
        lane = ScenarioLanes(
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            pdct_fam = e.pdct_fam,
            ori_name = e.ori_name,
            # ori_region = Column('ori_region', String)
            # ori_role = Column('ori_role', String, default='')
            # desti_name = Column('desti_name', String)
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
    engine = create_engine(DB_CONN_PARAMETER_CLOUD)
    Session = sessionmaker(bind=engine)
    session = Session()

    # location = session.query(Locations).first()
    # location2 = location
    # location2.name = 'hi'
    # print(location.name)

    scenario_id = 1
    baseline_id = 5

    print(session.query(Locations).filter(Locations.name == 'Karim'))

    # try:
    #     print(create_scenario(1, 5, "the 'nam scenario", session))
    #     add_alt_nodes(1, 5, node_dict, session)
    # except Exception as e:
    #     print(e)
    #     session.rollback()
    #     stmt = text("""
    #     delete from scdsi_scenarios where scenario_id = :scenario_id and baseline_id = :baseline_id
    #     """).params(
    #         scenario_id = scenario_id,
    #         baseline_id = baseline_id
    #     )
    #     session.execute(stmt)
    #     session.commit()

    session.close()