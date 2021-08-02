from AutoMap import *

from gurobipy import GRB

def get_pareto_weights():
    ret = []
    for i in range(0, 11):
        for j in range(0, 11 - i):
            ret += [(i / 10., j / 10., (10 - i - j) / 10.)]

    return ret

def print_optimal(model, index_to_node, index_to_mode):
    if model.Status == GRB.OPTIMAL:
        print("Variable Values")
        for v in model.getVars():
            if v.VarName[0] == 'X':
                ori, desti, mode = v.VarName.split(',')
                ori, desti, mode = index_to_node[int(ori[2:])]['name'], index_to_node[int(desti)]['name'], index_to_mode[int(mode[:-1])]
                print('<{}> | {}'.format((ori, desti, mode), v.X))

def write_optimal(model, index_to_node, index_to_mode, session):
    if model.Status == GRB.optimal:
        for v in model.getVars():
            None



if __name__ == '__main__':

    Session = sessionmaker(bind=engine)
    session = Session() 

    # pareto_weights = get_pareto_weights()
    # print(pareto_weights)

    scenario_id = 0
    baseline_id = 1

    for w in get_pareto_weights():
        run = Runs(
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            date = datetime.now(),
            description = 'one pf: QSFP40G',
            lambda_cost = w[0],
            lambda_time = w[1],
            lambda_co2e = w[2] 
        )

        session.add(run)
    session.commit()