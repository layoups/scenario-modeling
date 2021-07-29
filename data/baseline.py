import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker

from collections import deque

from datetime import datetime

from sqlalchemy.sql import base

from AutoMap import *
from PathRoles import *
from Eraser import *
from Visualize import *
from Alpha import *
from dfs import *
from edge_data import *
from node_data import *

def create_baseline(baseline_id, start, end, description, session):
    date = datetime.now()

    baseline = Baselines(baseline_id=baseline_id)
    baseline.start = start
    baseline.end = end
    baseline.description = description
    baseline.date = date
    session.add(baseline)

    scenario = Scenarios(scenario_id=0)
    scenario.baseline_id = baseline_id
    scenario.date = date
    scenario.description = description
    session.add(scenario)

    session.commit()

def populate_scenario_lanes(baseline_id, session):
    baseline = session.query(Baselines).filter(Baselines.baseline_id == baseline_id).first()

    stmt = text("""
        insert into scdsi_scenario_lanes 
        (scenario_row_id, scenario_id, baseline_id, pdct_fam, 
        ori_name, ori_country, ori_region, 
        desti_name, desti_country, desti_region, 
        ship_type, ship_rank, total_weight, total_paid)
        select row_id, 0, :baseline_id, product_family, 
        lower(ship_from_name), lower(ship_from_country), lower(ship_from_region_code),
        lower(ship_to_name), lower(ship_to_country), lower(ship_to_region_code),
        shipment_type, ship_rank,
        sum(billed_weight), sum(total_amount_paid_usd)
        from scdsi_cv_lane_rate_automation_pl rl join scdsi_ship_rank sr 
        on rl.shipment_type = sr.ship_type
        where billed_weight != 0 
        and shipment_type not in ('OTHER', 'BROKERAGE')
        and ship_date_pure_ship >= :start and ship_date_pure_ship <= :end
        and product_family not in ('TBA')
        and ship_from_name is not null
        and ship_from_country is not null
        and ship_from_region_code is not null
        and ship_to_name is not null
        and ship_to_country is not null
        and ship_to_region_code is not null
        group by row_id, product_family, 
        ship_from_name, ship_from_country, ship_from_region_code, 
        ship_to_name, ship_to_country, ship_to_region_code, 
        shipment_type, ship_rank;
    """).params(baseline_id = baseline_id, start = baseline.start, end = baseline.end)

    session.execute(stmt)
    session.commit()

def get_cost_omega(baseline_id, session):
    stmt = text("""
        insert into scdsi_omega (baseline_id, omega_cost)
        select baseline_id, sum(total_weight)
        from scdsi_scenario_lanes
        where baseline_id = :baseline_id
    """).params(baseline_id = baseline_id)

    session.execute(stmt)
    session.commit()

def set_baseline(baseline_id, start, end, description, session):
    create_baseline(baseline_id, start, end, description, session)
    populate_scenario_lanes(baseline_id, session)

    pdct_fam = 'PHONE'
    input('baseline + scenario lanes = ready for dfs?')
    dfs(baseline_id, pdct_fam, session)

    populate_scenario_edges(0, baseline_id, session)
    get_distances_time_co2e(0, baseline_id, session)
    set_in_pflow_for_scenario_edges(0, baseline_id, session)

    populate_baseline_nodes(baseline_id, session)
    get_node_supply(0, baseline_id, pdct_fam, session)
    get_node_capacity(0, baseline_id, pdct_fam, session)

    get_cost_omega(baseline_id, session)


if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()

    pdct_fam = 'PHONE'

    baseline_id = 1
    set_baseline(baseline_id, '2020-01-01', '2020-12-31', 'trial' , session)

    session.commit()


    