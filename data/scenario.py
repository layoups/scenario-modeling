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


def create_scenario(baseline_id, session):
    return None


def add_alt_nodes(scenario_id, baseline_id, node_dict, session):
    return None


def add_decom_nodes(scenario_id, baseline_id, node_dict, session):
    return None


def add_alt_edges(scenario_id, baseline_id, edge_dict, session):
    return None


def add_decom_edges(scenario_id, baseline_id, edge_dict, session):
    return None


if __name__ == '__main__':
    engine = create_engine(DB_CONN_PARAMETER_WI)
    Session = sessionmaker(bind=engine)
    session = Session()