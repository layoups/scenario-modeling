import gurobipy as gp
from gurobipy import GRB

from inputs import *

def run_model(objective_weights):
    model = gp.Model()

    X = model.addVars(
        [(i, j, m) for i, j, m in lanes],
        vtype=GRB.CONTINUOUS,
        lb=0.0,
        name="X",
    )

    O = model.addVars(
        [(n) for n in node_map],
        vtype=GRB.BINARY,
        name="O",
    )

    obj = objective_weights['cost'] * np.reciprocal(omega['cost']) * gp.quicksum((lanes[(i, j, m)]['transport_cost']) * X[i, j, m] for i, j, m in lanes)
    obj += objective_weights['time'] * np.reciprocal(omega['lead_time']) * gp.quicksum(lanes[(i, j, m)]['transport_time'] * X[i, j, m] for i, j ,m in lanes)
    obj += objective_weights['co2e'] * np.reciprocal(omega['co2e']) * gp.quicksum(lanes[(i,j,m)]['co2e'] * X[i, j, m] for i, j, m in lanes)
    model.setObjective(obj, GRB.MINIMIZE)

    # for j, p in customer_lanes:
    #     model.addConstr(
    #         -X.sum('*', j, p, '*') == -S[(j, p)]
    #     )

    # for j, p in gateway_lanes:
    #     model.addConstr(
    #         X.sum(j, '*', p, '*') - X.sum('*', j, p, '*') == S[(j, p)]
    #     )

    model.write('opt.lp')


if __name__ == '__main__':

    run_model(lamdas)
