import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker

from AutoMap import *

mapp = {
        0: "Supplier",
        1: "PCBA",
        2: "GHUB",
        3: "DF",
        4: "OSLC",
        5: "DSLC",
        6: "Gateway",
        7: "BTS",
        8: "ODM",
        9: "Customer",
    }

def poss_roles(ins, outs):
    Supp = not ins.any() and outs[0].any()
    PCBA = ins[[0, 2]].any() and outs[1].any()
    GHUB = ins[[0, 1]].any() and outs[2].any()
    DF = ins[[0, 1, 2]].any() and outs[[3, 4, 6]].any()
    OSLC = ins[3].any() and outs[[4, 5, 6]].any()
    DSLC = ins[4].any() and outs[[5, 6]].any()
    GWY = ins[5].any() and outs[6].any()
    BTS = ins[0].any() and outs[[4, 6]].any()
    ODM = ins[0].any() and outs[0].any()
    Cus = ins[6].any() and not outs.any()
    ret = np.array([Supp, PCBA, GHUB, DF, OSLC, DSLC, GWY, BTS, ODM, Cus])
    return ret if ret.any() else False


def actions(edge):
    po = edge.ship_type == "PO"
    otor1 = edge.ship_type == "OTOR1"
    dray = edge.ship_type == "DRAY"
    leg0 = edge.ship_type == "LEG0"
    leg1 = edge.ship_type == "LEG1"
    leg21 = edge.ship_type == "LEG2-1"
    leg22 = edge.ship_type == "LEG2-2"
    ret = np.array([po, otor1, dray, leg0, leg1, leg21, leg22])
    return ret


def path_confession(path, curr_path_head_rank, parent_pflow_head_rank):
    start = curr_path_head_rank + 1 if curr_path_head_rank > -1 else 1
    path[start - 1].in_pflow = 0
    for i in range(start, parent_pflow_head_rank):
        path[i].ori_role = None
        path[i].desti_role = None
        path[i].color = 0
        path[i].path = None
        path[i].pflow = None
        path[i].path_rank = None
        path[i].parent_pflow = None
        path[i].in_pflow = 0
    # session.commit()
    return False


def node_role_per_path(path, curr_path_head_rank, parent_pflow_head_rank, session):

    path_rng = np.arange(2 * len(path), dtype=int)
    working_rng = path_rng[2 * curr_path_head_rank + 2: 2 * parent_pflow_head_rank]

    ori_role = None
    desti_role = None

    for i in working_rng:
        p = i // 2
        if i == working_rng[-1]:
            if i < path_rng[-1]:
                ori_role = path[(i + 1) // 2].desti_role
            else:   
                role = poss_roles(
                    np.array([False, False, False, False, False, False, False]),
                    actions(path[p]),
                )
                if role is False:
                    return path_confession(path, curr_path_head_rank, parent_pflow_head_rank)
                ori_role = mapp[np.argmax(role)]
            path[p].ori_role = ori_role

        elif i == working_rng[0]:
            if i > path_rng[0]:
                desti_role = path[(i - 1) // 2].ori_role
            else:
                role = poss_roles(
                    actions(path[p]),
                    np.array([False, False, False, False, False, False, False]),
                )
                if role is False:
                    return path_confession(path, curr_path_head_rank, parent_pflow_head_rank)
                desti_role = mapp[np.argmax(role)]
            path[p].desti_role = desti_role

        elif i % 2 == 1:
            role = poss_roles(actions(path[p + 1]), actions(path[p]))
            if role is False:
                return path_confession(path, curr_path_head_rank, parent_pflow_head_rank)
            ori_role = mapp[np.argmax(role)]
            path[p].ori_role = ori_role
            desti_role = ori_role

        else:
            path[p].desti_role = desti_role
        path[p].in_pflow = 1
    # session.commit()
    return True

def get_parent_pflow(path, v, curr_path_head_rank, path_rank, session):
    role = poss_roles(actions(v), actions(path[-1]))
    if role is False:
        return role
    if mapp[np.argmax(role)] == v.desti_role:
        set_parent_pflow(path, v, curr_path_head_rank, path_rank, session)
        return True

def set_parent_pflow(path, parent_pflow_node, curr_path_head_rank, path_rank, session):
    for u in path:
        u.parent_pflow = parent_pflow_node.pflow
    # session.commit()
    node_role_per_path(path + [parent_pflow_node], curr_path_head_rank[0], path_rank, session)

def debug_print(stack, path, path_stack, path_rank, pflow, curr_path_head_rank, flag):
    print(flag)
    print(path_stack)

    for s in stack:
        print(s)
    
    print("path: {}, path_rank: {}, pflow: {}, curr_path_head_rank: {}".format(
        path[0], path_rank, pflow, curr_path_head_rank))
    input()
