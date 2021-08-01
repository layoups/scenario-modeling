import numpy as np
import pandas as pd
from sqlalchemy.sql import base
from sqlalchemy.sql.functions import mode

from AutoMap import *
from sqlalchemy import create_engine, desc, or_, text

from pprint import pprint

mode_to_index = {
    'Air': 1,
    'Truck': 2,
    'Ocean': 3,
    'Rail': 4
    }

index_to_mode = {
    1: 'Air',
    2: 'Truck',
    3: 'Ocean',
    4: 'Rail'
}

# Change node map to location+role, pdct
# no need for product as index for decision vars

if __name__ == '__main__':

    Session = sessionmaker(bind=engine)
    session = Session()

    scenario_id = 0
    baseline_id = 1

    node_map, node_to_index = ScenarioNodes.get_node_maps(scenario_id, baseline_id, session)

    pdct_to_index, index_to_pdct = ScenarioLanes.get_pdct_maps(scenario_id, baseline_id, session)

    lanes = ScenarioLanes.get_lanes(scenario_id, baseline_id, node_to_index, mode_to_index, session)

    # nodes = node_map['name']

    pprint(ScenarioLanes.get_manufacturing_adjacency_list(scenario_id, baseline_id, node_to_index, session))

    customer_lanes = {} 
    gateway_lanes = {}
    dslc_lanes = {} 
    oslc_lanes = {} 
    df_lanes = {} 
    ghub_lanes = {} 
    pcba_lanes = {}

    manufacturing_adjacency_list = {}

    # C = lanes['transport_cost'] # transportation cost
    # V = node_map['opex'] # transformation cost
    # E = lanes['co2e'] # co2e
    # T = lanes['transport_time'] # time

    # S = node_map['supply'] # supply
    # U = node_map['capacity'] # capacity
    alpha = {}