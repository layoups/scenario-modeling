import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker

from collections import deque

from datetime import datetime

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
    

# node_dict = {'pdct_fam': , 'name': , 'country': , 'region': , 'role': , 'capacity': , 'supply': , 'demand': }
def add_alt_nodes(scenario_id, baseline_id, node_dict, session):
    return None

# node_dict = {'pdct_fam': , 'name': , 'country': , 'region': , 'role': }
def add_decom_nodes(scenario_id, baseline_id, node_dict, session):
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
def add_alt_edges(scenario_id, baseline_id, edge_dict, session):
    return None


def add_decom_edges(scenario_id, baseline_id, edge_dict, session):
    return None


if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER_WI)
    Session = sessionmaker(bind=engine)
    session = Session()