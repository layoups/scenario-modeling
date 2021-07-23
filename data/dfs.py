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

Session = sessionmaker(bind=engine)

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

def create_scenario(scenario_id, baseline_id, date, description, session):
    scenario = Scenarios(scenario_id=scenario_id)
    scenario.baseline_id = baseline_id
    scenario.date = date
    scenario.description = description
    session.add(scenario)
    session.commit()


def populate_scenario_lanes(baseline_id, session):
    baseline = session.query(Baselines).filter(Baselines.baseline_id == baseline_id).first()

    stmt = text("""
        insert into "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" (scenario_row_id, scenario_id, baseline_id, pdct_fam, ori_name, ori_country, ori_region, desti_name, desti_country, desti_region, ship_type, ship_rank, total_weight, total_paid)
        select "ROW_ID", 0, :baseline_id, "PRODUCT_FAMILY", 
        lower("SHIP_FROM_NAME"), lower("SHIP_FROM_COUNTRY"), lower("SHIP_FROM_REGION_CODE"),
        lower("SHIP_TO_NAME"), lower("SHIP_TO_COUNTRY"), lower("SHIP_TO_REGION_CODE"),
        "SHIPMENT_TYPE", ship_rank,
        sum("BILLED_WEIGHT"), sum("TOTAL_AMOUNT_PAID")
        from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_CV_LANE_RATE_AUTOMATION_PL" rl join "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SHIP_RANK" sr 
        on rl."SHIPMENT_TYPE" = sr.ship_type
        where "BILLED_WEIGHT" != 0 
        and "SHIPMENT_TYPE" not in ('OTHER', 'BROKERAGE')
        and "SHIP_DATE_PURE_SHIP" >= :start and "SHIP_DATE_PURE_SHIP" <= :end
        and "PRODUCT_FAMILY" not in ('TBA')
        group by "ROW_ID", "PRODUCT_FAMILY", 
        "SHIP_FROM_NAME", "SHIP_FROM_COUNTRY", "SHIP_FROM_REGION_CODE", 
        "SHIP_TO_NAME", "SHIP_TO_COUNTRY", "SHIP_TO_REGION_CODE", 
        "SHIPMENT_TYPE", ship_rank;
    """).params(baseline_id = baseline_id, start = baseline.start, end = baseline.end)

    session.execute(stmt)
    session.commit()

def dfs(baseline_id, pdct_fam):
    print('Network Reconstruction for {}'.format(pdct_fam))

    session = Session()
    graph = session.query(ScenarioLanes).filter(
        ScenarioLanes.pdct_fam == pdct_fam,
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id,
        ).order_by(desc(ScenarioLanes.ship_rank))

    # pdct_types = session.query(pdct_fam_types).filter(pdct_fam_types.pdct_fam == pdct_fam)
    # pdct_type = pdct_types.first()

    # for pdct_type in pdct_types:
    for v in graph.all():
        v.color = 0
    session.commit()
    
    time = [0]
    pflow = 1

    pflow_heads = []
    stack = []
    path_stack = deque([1, 1], maxlen=2)

    start = datetime.now()

    for v in graph.all():
        if v.color == 0:
            path = [1]
            path_rank = 0
            curr_path_head_rank = [-1]
            stack += [v]
            dfs_visit(baseline_id, pdct_fam, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session)
        pflow += 1
        input("new pflow-----------------------------------------------------------------------new pflow")
    print("Network Revealed for {}".format(pdct_fam))
    print(datetime.now() - start)


def dfs_visit(baseline_id, pdct_type, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session):
    v = stack[-1]

    v.scenario_id = 0
    v.baseline_id = baseline_id
    v.pflow = pflow
    v.path = path[0]
    v.path_rank = path_rank

    time[0] += 1
    v.d = time[0]
    v.color = 1

    debug_print(stack, path, path_stack, path_rank, pflow, curr_path_head_rank[0], "HEAD")

    if path[0] > 1 and path_stack[0] < path_stack[1]:
        curr_path_head_rank[0] = path_rank - 1

    path_rank += 1

    result = session.query(ScenarioLanes).filter(
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id,
        ScenarioLanes.desti_name == v.ori_name, 
        ScenarioLanes.desti_country == v.ori_country,
        ScenarioLanes.desti_region == v.ori_region,
        ScenarioLanes.ship_rank <= v.ship_rank,
        ScenarioLanes.ship_type != v.ship_type,
        ScenarioLanes.pdct_fam == v.pdct_fam 
        ).order_by(desc(ScenarioLanes.ship_rank), desc(ScenarioLanes.pflow))


    to_path = True
    for u in result.all():
        to_path = False
        if u.color == 0:
            path_stack.append(1)
            stack += [u]
            dfs_visit(scenario_id, baseline_id, pdct_type, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session)
        if u.pflow and u.pflow < pflow and u.color == 2:
            get_parent_pflow(stack, u, curr_path_head_rank, path_rank, session)

    v.color = 2
    time[0] += 1
    v.f = time[0]

    if to_path:
        if node_role_per_path(stack, curr_path_head_rank[0], len(stack), session):
            path[0] += 1
            debug_print(stack, path, path_stack, path_rank, pflow, curr_path_head_rank[0], "TAIL")
    stack.pop()
    path_rank -= 1
    path_stack.append(0)

    session.commit()



if __name__ == "__main__":

    # print(ScenarioLanes.__table__.columns.keys())

    pdct_fam = "4400ISR"
    scenario_id = 0
    baseline_id = 1

    # create_baseline(baseline_id, '2020-01-01', '2020-12-31', 'trial', Session())
    # populate_scenario_lanes(baseline_id, Session())

    # erase([pdct_fam], Session(), ScenarioLanes)
    
    dfs(baseline_id, pdct_fam)
    # get_alphas(pdct_fam, Session())
    # visualize_networkx(pdct_fam, Session())
    # visualize_graphivz(pdct_fam, Session())
