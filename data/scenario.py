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
# } USE SQL
def add_alt_nodes(scenario_id, baseline_id, node_dict, session):
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
        insert into :insert_table 
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
        from :select_table
            where scenario_id in (:scenario_id)
                and baseline_id in (:baseline_id)
                and pdct_fam in (:pdct_fam)
                and ori_name in (:ori_name)
                and ori_country in (:ori_country)
                and ori_region in (:ori_region)
                and ori_role in (:ori_role)
        """
    ).params(
        insert_table = ScenarioLanes.__tablename__,
        alt_name = alt_node.alt_name,
        alt_country = alt_node.alt_country,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        select_table = ScenarioLanes.__tablename__,
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
        insert into :insert_table 
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
        from :select_table
            where scenario_id in (:scenario_id)
                and baseline_id in (:baseline_id)
                and pdct_fam in (:pdct_fam)
                and desti_name in (:desti_name)
                and desti_country in (:desti_country)
                and desti_region in (:desti_region)
                and desti_role in (:desti_role)
        """
    ).params(
        insert_table = ScenarioLanes.__tablename__,
        alt_name = alt_node.alt_name,
        alt_country = alt_node.alt_country,
        alt_region = alt_node.alt_region,
        alt_role = alt_node.role,
        select_table = ScenarioLanes.__tablename__,
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
        update :table 
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
        table = DecomNodes.__tablename__,
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
        update :table
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
        table = DecomNodes.__tablename__,
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


if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER_WI)
    Session = sessionmaker(bind=engine)
    session = Session()

    location = session.query(Locations).first()
    location2 = location
    # location2.name = 'hi'
    print(location.name)

    session.close()