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
            
            else:
                if v.X == 1:
                    node = v.VarName[2:-1]
                    node = index_to_node[int(node)]['name']
                    print("<{}> | {}".format(node, v.X))

def write_optimal(model, run_id, scenario_id, baseline_id, index_to_node, index_to_mode, lanes, session):
    if model.Status == GRB.OPTIMAL:
        total_cost = 0
        total_time = 0
        total_co2e = 0
        for v in model.getVars():
            if v.VarName[0] == 'X':
                ori, desti, mode = v.VarName.split(',')
                ori, desti, mode = int(ori[2:]), int(desti), int(mode[:-1])
                
                total_cost += lanes[(ori, desti, mode)]['transport_cost'] * v.X
                total_time += lanes[(ori, desti, mode)]['transport_time'] * v.X
                total_co2e += lanes[(ori, desti, mode)]['co2e'] * v.X
                
                ori, desti, mode = index_to_node[ori]['name'], index_to_node[desti]['name'], index_to_mode[mode]
                ori_name, ori_region, ori_role = ori[1], ori[2], ori[3]
                desti_name, desti_region, desti_role = desti[1], desti[2], desti[3]
                pdct_fam = ori[0]
                opti_flow = OptimalFlows(
                    run_id = run_id,
                    scenario_id = scenario_id,
                    baseline_id = baseline_id,
                    pdct_fam = pdct_fam,
                    ori_name = ori_name,
                    ori_region = ori_region,
                    ori_role = ori_role,
                    desti_name = desti_name,
                    desti_region = desti_region,
                    desti_role = desti_role,
                    transport_mode = mode,
                    flow = v.X
                )
                session.add(opti_flow)
            
            else:
                node = index_to_node[int(v.VarName[2:-1])]['name']
                pdct_fam, name, region, role = node
                
                opti_node = OptimalNodes(
                    run_id = run_id,
                    scenario_id = scenario_id,
                    baseline_id = baseline_id,
                    pdct_fam = pdct_fam,
                    name = name,
                    region = region,
                    role = role,
                    state = v.X
                )
                session.add(opti_node)
            session.commit()
        solution = Solution(
            run_id = run_id,
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            optimal_cost = total_cost,
            optimal_time = total_time,
            optimal_co2e = total_co2e
        )
        session.add(solution)
        session.commit()
    else:
        return False



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