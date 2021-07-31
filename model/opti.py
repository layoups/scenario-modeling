import gurobipy as gp
from gurobipy import GRB

from inputs import *

def run_model(objective_weights):
    model = gp.Model()

    X = model.addVars(
        [(i, j, p, m) for i, j, p, m in lanes],
        vtype=GRB.CONTINUOUS,
        lb=0.0,
        name="X",
    )

    O = model.addVars(
        [(n, p) for n, p in nodes],
        vtype=GRB.BINARY,
        name="O",
    )

    for j, p in customer_lanes:
        model.addConstr(
            -X.sum('*', j, p, '*') == -S[(j, p)]
        )

    for j, p in gateway_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )

    for j, p in oslc_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )

    for j, p in dslc_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )
    
    for j, p in df_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )

    for j, p in ghub_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )

    for j, p in pcba_lanes:
        model.addConstr(
            X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
        )

    for j, p in dslc_lanes:
        model.addConstr(
            X.sum('*', j, p, '*') <= U[(j, p)] * O[j, p]
        )


run_model(0)
