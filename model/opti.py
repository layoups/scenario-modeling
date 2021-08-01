import gurobipy as gp
from gurobipy import GRB

from inputs import *

# C = lanes[(ori_index, desti_index, mode_index)]['transport_cost'] # transportation cost
# V = node_map[node_index]['opex'] # transformation cost
# E = lanes[(ori_index, desti_index, mode_index)]['co2e'] # co2e
# T = lanes[(ori_index, desti_index, mode_index)]['transport_time'] # time

# S = node_map[node_index]['supply'] # supply
# U = node_map[node_index]['capacity'] # capacity
# index_to_node = node_map[node_index]['name']
# alpha = {} manufacturing_adjacency_list[manuf_index][d][i][-1]

# MODEL IS CURRENTLY CONSTRAINED TO MASTER PFLOWS
# CHANGE ALPHA TO MAKE SURE SUM = 1

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

    for role in specified_lanes:
        if role == 'Customer':
            model.addConstrs(
                (-X.sum('*', j, '*') == node_map[j]['supply'] for j in specified_lanes[role]),
                name = 'customer_flow'
            )
        else:
            model.addConstrs(
                (X.sum(j, '*', '*') - X.sum('*', j, '*') == node_map[j]['supply'] for j in specified_lanes[role]),
                name = 'non_customer_flow'
            )
            if role != 'Gateway':
                model.addConstrs(
                    (X.sum('*', j, '*') <= node_map[j]['capacity'] * O[j] for j in specified_lanes[role]), # node_map[j]['capacity']
                    name = 'capacity_constraint'
                )

    for j in manufacturing_adjacency_list:
        adj = manufacturing_adjacency_list[j]
        model.addConstrs(
            (np.sum([X.sum(x[0], j, '*') for x in adj[d]]) == adj[d][0][-1] * (X.sum(j, '*', '*') - node_map[j]['supply']) for d in adj),
            name = 'alpha_constraint'
        )


    model.write('model/opt.lp')
    model.optimize()


if __name__ == '__main__':

    run_model(lamdas)
