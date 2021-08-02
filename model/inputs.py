import numpy as np
import pandas as pd
from sqlalchemy.sql import base
from sqlalchemy.sql.functions import mode

from AutoMap import *
from sqlalchemy import create_engine, desc, or_, text

from pprint import pprint

Session = sessionmaker(bind=engine)
session = Session()

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


scenario_id = 0
baseline_id = 1
run_id = session.query(Runs.run_id).filter(
    Runs.scenario_id == scenario_id,
    Runs.baseline_id == baseline_id,
    Runs.lambda_cost == 0.5,
    Runs.lambda_time == 0.2,
    Runs.lambda_co2e == 0.3
).first().run_id

node_map, node_to_index = ScenarioNodes.get_node_maps(scenario_id, baseline_id, session)

lanes = ScenarioLanes.get_lanes(scenario_id, baseline_id, node_to_index, mode_to_index, session)

manufacturing_adjacency_list = ScenarioLanes.get_manufacturing_adjacency_list(scenario_id, baseline_id, node_to_index, session)

specified_lanes = ScenarioLanes.get_specified_lanes(scenario_id, baseline_id, node_to_index, session)

omega = Omega.get_omegas(baseline_id, session) # cost, lead_time, co2e

lamdas = Runs.get_lambdas(run_id, scenario_id, baseline_id, session) # cost, time, co2e

if __name__ == '__main__':
    print(node_map[456699]['name'])

    # pprint(manufacturing_adjacency_list)

    # pprint(lanes)

    # pprint(node_map)

    # pprint(specified_lanes)

    # pprint(omega)

    # pprint(lamdas)