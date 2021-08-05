from AutoMap import *

from gurobipy import GRB

from pprint import pprint

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
        total_flow = 0
        for v in model.getVars():
            if v.VarName[0] == 'X':
                ori, desti, mode = v.VarName.split(',')
                ori, desti, mode = int(ori[2:]), int(desti), int(mode[:-1])
                
                total_cost += lanes[(ori, desti, mode)]['transport_cost'] * v.X
                total_time += lanes[(ori, desti, mode)]['transport_time'] * v.X
                total_co2e += lanes[(ori, desti, mode)]['co2e'] * v.X
                total_flow += v.X
                
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
            optimal_co2e = total_co2e,
            total_flow = total_flow,
            solution = model.objval
        )
        session.add(solution)
        session.commit()
    else:
        return False

def get_mode_mix(scenario_id, baseline_id, session, run_id=None):
    stmt = text(
        """
        select a.run_id, transport_mode, cast(sum(flow) as float) / b.total * 100 as mix
        from scdsi_optimal_flows a
        join(
            select run_id, scenario_id, baseline_id, sum(flow) as total
            from scdsi_optimal_flows
            group by run_id, scenario_id, baseline_id
        ) as b
        on a.baseline_id = b.baseline_id and a.scenario_id = b.scenario_id and a.run_id = b.run_id
        where a.baseline_id = :baseline_id
        and a.scenario_id = :scenario_id
        group by a.run_id, a.transport_mode, b.total
        order by a.run_id
        """
    ).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    mode_mix = session.execute(stmt).all()   
    ret = {}
    run = 0
    for x in mode_mix:
        if x.run_id != run:
            ret[x.run_id] = {}
            run = x.run_id
        ret[x.run_id][x.transport_mode] = round(x.mix, 2)

    stmt = text("""
        select sub.transport_mode, 
        cast(sum(sub.total_weight) as float) as mode_weight 
        from (select e.ori_name, e.ori_region,
			  e.desti_name, e.desti_region, e.total_weight, e.transport_mode
            from scdsi_scenario_lanes l
            left join scdsi_scenario_edges e
            on l.baseline_id = e.baseline_id
            and l.scenario_id = e.scenario_id
            and l.ori_name = e.ori_name
            and l.ori_region = e.ori_region
            and l.desti_name = e.desti_name
            and l.desti_region = e.desti_region
            where l.in_pflow = 1  
            and l.alpha is not null 
            and l.baseline_id = :baseline_id
            and l.scenario_id = :scenario_id
            -- and l.parent_pflow is null
            ) as sub
		group by sub.transport_mode
    """).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    mode_mix = session.execute(stmt).all()
    total = np.sum([x.mode_weight for x in mode_mix])
    ret_baseline = {x.transport_mode: 100 * round(x.mode_weight / total, 2) for x in mode_mix}
    return ret, ret_baseline

def get_kpi_ranges(scenario_id, baseline_id, session):
    stmt = text(
        """
        select cast(max(optimal_cost) as float) / min(optimal_cost) as cost_range, 
        cast(max(optimal_time) as float) / min(optimal_time) as time_range,
        cast(max(optimal_co2e) as float) / min(optimal_co2e) as co2e_range
        from scdsi_solution
        where scenario_id = :scenario_id
        and baseline_id = :baseline_id
        """
    ).params(
        scenario_id = scenario_id,
        baseline_id = baseline_id
    )
    ret = session.execute(stmt).first()
    return {'cost': round(ret.cost_range, 2), 'time': round(ret.time_range, 2), 'co2e': round(ret.co2e_range, 2)}

def get_kpi_per(scenario_id, baseline_id, session, lambda_cost=None, lambda_co2e=None, lambda_time=None):
    if lambda_cost == None:
        stmt = text(
            """
            select min(optimal_cost)/ min(total_flow) as optimal_cost, 
            min(optimal_time) / min(total_flow) as optimal_time, 
            min(optimal_co2e) / min(total_flow)as optimal_co2e
            from scdsi_solution
            where scenario_id = :scenario_id
            and baseline_id = :baseline_id
            """
        ).params(
            scenario_id = scenario_id,
            baseline_id = baseline_id
        )
    else:
        stmt = text(
            """
            select optimal_cost/ total_flow as optimal_cost, 
            optimal_time / total_flow as optimal_time, 
            optimal_co2e / total_flow as optimal_co2e
            from scdsi_solution s
                join scdsi_runs r
                on s.run_id = r.run_id
                and s.scenario_id = r.scenario_id
                and s.baseline_id = r.baseline_id
            where s.scenario_id = :scenario_id
            and s.baseline_id = :baseline_id
            and r.lambda_cost = :lambda_cost
            and r.lambda_time = :lambda_time
            and r.lambda_co2e = :lambda_co2e
            """
        ).params(
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            lambda_cost = lambda_cost,
            lambda_co2e = lambda_co2e,
            lambda_time = lambda_time
        )
    x = session.execute(stmt).first()
    scenario_kpis = {'scenario_cost': round(x.optimal_cost, 3), 'scenario_time': round(x.optimal_time, 3), 'scenario_co2e': round(x.optimal_co2e, 3)}

    stmt = text(
        """
        select baseline_cost / total_flow as cost_per,
        baseline_lead_time / total_flow as time_per,
        baseline_co2e / total_flow as co2e_per
        from scdsi_omega
        where baseline_id = :baseline_id
        """
    ).params(
        baseline_id = baseline_id
    )
    x = session.execute(stmt).first()
    baseline_kpis = {'baseline_cost': round(x.cost_per, 3), 'baseline_time': round(x.time_per, 3), 'baseline_co2e': round(x.co2e_per, 3)}

    return baseline_kpis, scenario_kpis


if __name__ == '__main__':

    Session = sessionmaker(bind=engine)
    session = Session() 

    # pareto_weights = get_pareto_weights()
    # print(pareto_weights)

    scenario_id = 0
    baseline_id = 9

    for w in get_pareto_weights():
        run = Runs(
            scenario_id = scenario_id,
            baseline_id = baseline_id,
            date = datetime.now(),
            description = "'nam basket",
            lambda_cost = w[0],
            lambda_time = w[1],
            lambda_co2e = w[2] 
        )

        session.add(run)
    session.commit()

    # pprint(get_mode_mix(scenario_id, baseline_id, session))
    # pprint(get_kpi_ranges(scenario_id, baseline_id, session))
    # pprint(get_kpi_per(scenario_id, baseline_id, session)) #, lambda_co2e=0, lambda_time=1, lambda_cost=0))