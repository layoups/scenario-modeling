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
        (scenario_id, baseline_id, pdct_fam, 
        ori_name, ori_country, ori_region, 
        desti_name, desti_country, desti_region, 
        ship_type, ship_rank, total_weight, total_paid)
        select 0, :baseline_id, product_family, 
        ori_name, ori_country, ori_region, 
        desti_name, desti_country, desti_region, 
        ship_type, ship_rank, total_weight, total_paid
        from
        (select product_family, 
        lower(ship_from_name) as ori_name, lower(ship_from_country) as ori_country, lower(ship_from_region_code) as ori_region,
        lower(ship_to_name) as desti_name, lower(ship_to_country) as desti_country, lower(ship_to_region_code) as desti_region,
        shipment_type,
        sum(billed_weight) as total_weight, sum(total_amount_paid_usd) as total_paid
        from scdsi_cv_lane_rate_automation_pl 
        where billed_weight != 0 
        and shipment_type not in ('OTHER', 'BROKERAGE')
        and ship_date_pure_ship >= :start and ship_date_pure_ship <= :end
        and product_family not in ('TBA')
        and ship_from_name is not null
        and ship_from_country is not null
        and ship_from_region_code is not null
        and length(ship_from_name) > 3
        and ship_to_name is not null
        and ship_to_country is not null
        and ship_to_region_code is not null
        and length(ship_to_name) > 3
        group by product_family, 
        ship_from_name, ship_from_country, ship_from_region_code, 
        ship_to_name, ship_to_country, ship_to_region_code, 
        shipment_type) as rl join scdsi_ship_rank sr 
        on rl.shipment_type = sr.ship_type
    """).params(baseline_id = baseline_id, start = baseline.start, end = baseline.end)

    session.execute(stmt)
    session.commit()

def get_cost_omega(baseline_id, session):
    stmt = text("""
        insert into scdsi_omega (baseline_id, baseline_cost)
        select baseline_id, sum(total_weight)
        from scdsi_scenario_lanes
        where baseline_id = :baseline_id
        and scenario_id = 0
        and in_pflow = 1
        group by baseline_id, scenario_id
    """).params(baseline_id = baseline_id)

    session.execute(stmt)
    session.commit()

def set_baseline(baseline_id, start, end, description, session):
    try:
        create_baseline(baseline_id, start, end, description, session)
        populate_scenario_lanes(baseline_id, session)
        # input('baseline + scenario lanes = ready for dfs?')
        session.commit()
        print("Baseline Created")

    except Exception as e:
        session.rollback()
        print(e)
        print("failed to create baseline")
        stmt = text(
            """
            delete from scdsi_baselines where baseline_id = :baseline_id
            """
        ).params(
            baseline_id = baseline_id
        )
        session.execute(stmt)
        return session.commit()
    
    pdct_fams = ['AIRANT']

    try:
        for pdct_fam in pdct_fams: 
            dfs(baseline_id, pdct_fam, session)
            get_customer_alphas(0, baseline_id, pdct_fam, session)
            get_alphas(0, baseline_id, pdct_fam, session)
        session.commit()

        print("The Network is Reconstructed")

    except Exception as e:
        session.rollback()
        print(e)
        print("Network Resisted Reconstruction")
        stmt = text(
            """
            delete from scdsi_scenario_lanes 
            where baseline_id = :baseline_id
            and scenario_id = 0
            """
        ).params(
            baseline_id = baseline_id
        )
        session.execute(stmt)
        return session.commit()

    try:       
        populate_scenario_edges(0, baseline_id, session)
        get_distances_time_co2e(0, baseline_id, session)
        # set_in_pflow_for_scenario_edges(0, baseline_id, session)
        session.commit()

        print("Baeline Edges are Populated")

    except Exception as e:
        session.rollback()
        print(e)
        print("failed to populate scenario edges")
        stmt = text(
            """
            delete from scdsi_scenario_edges 
            where baseline_id = :baseline_id
            and scenario_id = 0
            """
        ).params(
            baseline_id = baseline_id
        )
        session.execute(stmt)
        return session.commit()

    try:
        populate_baseline_nodes(baseline_id, session)
        for pdct_fam in pdct_fams:
            get_node_supply(0, baseline_id, pdct_fam, session)
            get_node_capacity(0, baseline_id, pdct_fam, session)
        session.commit()

        print("Scenario Nodes are Populated")

    except Exception as e:
        session.rollback()
        print(e)
        print("failed to populate nodes")
        stmt = text(
            """
            delete from scdsi_scenario_nodes 
            where baseline_id = :baseline_id
            and scenario_id = 0
            """
        ).params(
            baseline_id = baseline_id
        )
        session.execute(stmt)
        return session.commit()

    try:
        get_cost_omega(baseline_id, session)
        session.commit()

        print("Omegas are Calculated")

    except Exception as e:
        session.rollback()
        print(e)
        print("failed to get omegas")
        stmt = text(
            """
            delete from scdsi_omega 
            where baseline_id = :baseline_id
            """
        ).params(
            baseline_id = baseline_id
        )
        session.execute(stmt)
        return session.commit()

    finally:
        print("Successfully created baseline!")
    


if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()

    pdct_fam = 'PHONE'

    start = datetime.now()

    baseline_id = 1
    set_baseline(baseline_id, '2019-01-01', '2020-12-31', 'trial' , session)

    print(datetime.now() - start)

    session.commit()


    