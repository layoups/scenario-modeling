from os import name
from AutoMap import *

from sqlalchemy import create_engine, desc, func
from sqlalchemy.orm import sessionmaker

import networkx as nx
import matplotlib.pyplot as plt
import graphviz as gv

colors = {
        "Supplier": "c",
        "PCBA": "b",
        "GHUB": "y",
        "DF": "m",
        "OSLC": "y",
        "DSLC": "y",
        "Gateway": "y",
        "BTS": "r",
        "ODM": "r",
        "Customer": "k",
    }

def get_main_pflow(scenario_id, baseline_id, pdct_fam, session):
    graph = session.query(ScenarioLanes).filter(
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id,
        ScenarioLanes.parent_pflow == None , 
        ScenarioLanes.pdct_fam == pdct_fam, 
        ScenarioLanes.in_pflow == 1).order_by(
            ScenarioLanes.path, 
            ScenarioLanes.ship_rank)
    return graph

def visualize_networkx(pdct_fam, session):
    graph = get_main_pflow(pdct_fam, session)

    G = nx.DiGraph()

    node_types = {
            "Supplier": set([]),
            "PCBA": set([]),
            "GHUB": set([]),
            "DF": set([]),
            "OSLC": set([]),
            "DSLC": set([]),
            "Gateway": set([]),
            "BTS": set([]),
            "ODM": set([]),
            "Customer": set([]),
        }

    path = []
    path_num = 1

    destination = 'cisco'
    desti_role = 'cisco'

    # no need for set in node_types

    for e in graph.all():
        origin = "{}_{}_{}_{}".format(e.ori_name, e.ori_country, e.ori_region, e.ori_role)
        node_types[e.ori_role].add(origin)
        if e.path > path_num:
            node_types[desti_role].add(destination)
            path += [destination]
            nx.add_path(G, path)
            path = [origin]
        else:
            path += [origin]
        path_num = e.path
        desti_role = e.desti_role
        destination = "{}_{}_{}_{}".format(e.desti_name, e.desti_country, e.desti_region, desti_role)
    path += [destination]
    nx.add_path(G, path)

    pos = nx.planar_layout(G)

    for n in node_types:
        nodes = node_types[n]
        if nodes:
            nx.draw_networkx_nodes(G, pos, nodelist=node_types[n], node_color=colors[n])


    nx.draw_networkx_labels(G, pos)
    nx.draw_networkx_edges(G, pos)

    # nx.draw(G, pos)
    plt.tight_layout()
    plt.show()

def visualize_graphivz(scenario_id, baseline_id, pdct_fam, session):
    graph = get_main_pflow(scenario_id, baseline_id, pdct_fam, session)

    G = gv.Digraph(pdct_fam, node_attr={'color': 'lightblue2', 'style': 'filled'})
    # G.attr(size='6,6')

    for e in graph.all():
        # origin = "{}\n{}\n{}\n{}".format(e.ori_name, e.ori_country, e.ori_region, e.ori_role)
        # destination = "{}\n{}\n{}\n{}".format(e.desti_name, e.desti_country, e.desti_region, e.desti_role)

        origin = "{}\n{}".format(e.ori_name, e.ori_role)
        destination = "{}\n{}".format(e.desti_name, e.desti_role)

        G.edge(origin, destination, label=str(round(e.alpha, 5)))

    G.unflatten(stagger=5).view()
    # G.view()

def visualize_solution(scenario_id, baseline_id, pdct_fam, lambda_cost, lambda_co2, lambda_time, session):
    
    run_id = session.query(Runs.run_id).filter(
        Runs.scenario_id == scenario_id,
        Runs.baseline_id == baseline_id,
        Runs.lambda_cost == lambda_cost,
        Runs.lambda_time == lambda_time,
        Runs.lambda_co2e == lambda_co2
    ).first().run_id

    graph = session.query(OptimalFlows).filter(
        OptimalFlows.run_id == run_id,
        OptimalFlows.scenario_id == scenario_id,
        OptimalFlows.baseline_id == baseline_id,
        OptimalFlows.pdct_fam == pdct_fam,
        OptimalFlows.desti_role != 'Customer'
    )

    G = gv.Digraph('{}_optimal'.format(pdct_fam), node_attr={'color': 'lightblue2', 'style': 'filled'})
    F = gv.Digraph('{}_no_flow'.format(pdct_fam), node_attr={'color': 'lightblue2', 'style': 'filled'})
    # G.attr(size='6,6')

    for e in graph.all():
        # origin = "{}\n{}\n{}\n{}".format(e.ori_name, e.ori_country, e.ori_region, e.ori_role)
        # destination = "{}\n{}\n{}\n{}".format(e.desti_name, e.desti_country, e.desti_region, e.desti_role)

        origin = "{}\n{}".format(e.ori_name, e.ori_role)
        destination = "{}\n{}".format(e.desti_name, e.desti_role)

        if e.flow > 0 or e.ori_name == 'hanoi,vn' or e.desti_name == 'hanoi,vn':
            g_label = '{} | {}'.format(e.transport_mode, e.flow)
            G.edge(origin, destination, label=g_label)

        f_label = '{}'.format(e.transport_mode)
        F.edge(origin, destination, label=f_label)

    G.unflatten(stagger=5).view()
    F.unflatten(stagger=5).view()
    # G.view()

def visualize_alt_paths(pdct_fam, var_node, alts, session, leech):
    from graphviz import Digraph
    graph = get_main_pflow(pdct_fam, session)
    graph_name = '{}_alt_{}'.format(pdct_fam, var_node)
    G = Digraph(graph_name, node_attr={'color': 'lightblue2', 'style': 'filled'}, engine='dot')
    G.attr(compound='true', rankdir='LR')

    cluster_name = 'cluster_{}'.format(var_node)

    with G.subgraph(name=cluster_name) as a:
        for i in alts:
            a.node(i)

    for e in graph.all():
        origin = "{}\n{}".format(e.ori_name, e.ori_role)
        destination = "{}\n{}".format(e.desti_name, e.desti_role)

        if e.desti_role == var_node:
            with G.subgraph(name=cluster_name) as a:
                a.node(destination)
            G.edge(origin, destination, lhead=cluster_name, label=str(e.alpha))

        elif e.ori_role == var_node:
            G.edge(origin, destination, ltail=cluster_name, label=str(e.alpha))

        else:
            G.edge(origin, destination, label=str(e.alpha)) 

    if leech:
        G.edge(alts[-1], 'leech', ltail=cluster_name, label='N(mu, sigma^2)')
    # G.unflatten(stagger=5).view()
    # G.view()

if __name__ == "__main__":

    # from graphviz import Source
    # Source.from_file('file.gv')


    engine = create_engine(DB_CONN_PARAMETER_CLOUD)
    Session = sessionmaker(bind=engine)
    session = Session()

    scenario_id = 0
    baseline_id = 9

    pdct_fam = 'C4500'
    # visualize_networkx(pdct_fam, session)
    visualize_graphivz(scenario_id, baseline_id, pdct_fam, session)
    # visualize_solution(scenario_id, baseline_id, pdct_fam, lambda_cost=0, lambda_co2=1, lambda_time=0, session=session)

    # alt_names = ['DF', 'PCBA']
    # for alt_name in alt_names:
    #     visualize_alt_paths(pdct_fam, alt_name, ['alt_{}_{}'.format(alt_name, i) for i in range(1)], session, True)
    #     visualize_alt_paths(pdct_fam, alt_name, ['alt_{}_{}'.format(alt_name, i) for i in range(1)], session, True)

