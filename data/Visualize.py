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

        G.edge(origin, destination, label=str(e.alpha))

    G.unflatten(stagger=5).view()
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
    baseline_id = 2

    pdct_fam = 'SFP10G'
    # visualize_networkx(pdct_fam, session)
    visualize_graphivz(scenario_id, baseline_id, pdct_fam, session)

    # alt_names = ['DF', 'PCBA']
    # for alt_name in alt_names:
    #     visualize_alt_paths(pdct_fam, alt_name, ['alt_{}_{}'.format(alt_name, i) for i in range(1)], session, True)
    #     visualize_alt_paths(pdct_fam, alt_name, ['alt_{}_{}'.format(alt_name, i) for i in range(1)], session, True)

