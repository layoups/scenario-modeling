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

def dfs(scenario_id, baseline_id, pdct_fam):
    print('Network Reconstruction for {}'.format(pdct_fam))

    session = Session()
    graph = session.query(RawLanes).filter(
        RawLanes.pdct_fam == pdct_fam,
        RawLanes.scenario_id == scenario_id,
        RawLanes.baseline_id == baseline_id,
        ).order_by(desc(RawLanes.ship_rank))

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
            dfs_visit(scenario_id, baseline_id, pdct_fam, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session)
        pflow += 1
        # input("new pflow-----------------------------------------------------------------------new pflow")
    print("Network Revealed for {}".format(pdct_fam))
    print(datetime.now() - start)


def dfs_visit(scenario_id, baseline_id, pdct_type, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session):
    v = stack[-1]

    # debug_print(stack, path, path_stack, path_rank, pflow, curr_path_head_rank[0], "HEAD")

    v.scenario_id = scenario_id
    v.baseline_id = baseline_id
    v.pflow = pflow
    v.path = path[0]
    v.path_rank = path_rank

    time[0] += 1
    v.d = time[0]
    v.color = 1

    if path[0] > 1 and path_stack[0] < path_stack[1]:
        curr_path_head_rank[0] = path_rank - 1

    path_rank += 1

    result = session.query(Lanes).filter(
        Lanes.desti_name == v.ori_name, 
        Lanes.desti_country == v.ori_country,
        Lanes.desti_region == v.ori_region,
        Lanes.ship_rank <= v.ship_rank,
        Lanes.ship_type != v.ship_type,
        Lanes.pdct_fam == v.pdct_fam 
        ).order_by(desc(Lanes.ship_rank), desc(Lanes.pflow))


    to_path = True
    for u in result.all():
        to_path = False
        if u.color == 0:
            path_stack.append(1)
            stack += [u]
            dfs_visit(pdct_type, stack, pflow, path_stack, curr_path_head_rank, path, path_rank, time, pflow_heads, session)
        if u.pflow and u.pflow < pflow and u.color == 2:
            get_parent_pflow(stack, u, curr_path_head_rank, path_rank, session)

    v.color = 2
    time[0] += 1
    v.f = time[0]

    if to_path:
        if node_role_per_path(stack, curr_path_head_rank[0], len(stack), session):
            path[0] += 1
            # debug_print(stack, path, path_stack, path_rank, pflow, curr_path_head_rank[0], "TAIL")
    stack.pop()
    path_rank -= 1
    path_stack.append(0)

    session.commit()



if __name__ == "__main__":

    pdct_fam = "4400ISR"
    erase([pdct_fam], Session(), Lanes)
    dfs(0, 1, pdct_fam)
    # get_alphas(pdct_fam, Session())
    # visualize_networkx(pdct_fam, Session())
    # visualize_graphivz(pdct_fam, Session())
