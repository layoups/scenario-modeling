import graphviz as gv

################### Restructuring to reflect Historical + New Scenario Def --> MILP ###################

#### nodes

raw_data = ['Raw Lane\nData', 'FY2020']
nodes = ['Node\nCharacteristics']
trsfm_costs = ['Total\nOPEX\nper\nNode', 'Total\nOPEX\nper\nNode']
trsprt_times = ['Transportation\nTimes', 'T[ijm]']
fin = ['Finance']
cpx = ['CAPEX', 'R[n]']
frcst_dem = ['Forecasted\nDemand', 'forecasted_demand']
ntwrk_struct = ['Network\nStructure', 'Z']
ntwrk = ['Network', 'Network']
co_two = ['Emission\nStandards', 'carbon_factors']
edges = ['Edge\nCharacteristics', 'X[ijpm]']
milp = ['MILP', 'gurobi']
scenario = ['Scenario\nDefinition', 'alt_sets']
risk = ['Risk', 'Risk']
leech = ['Leech\nNodes', 'set L']
gtc = ['GTC', 'GTC']
opti_state = ['Optimal\nState', 'optimal\nX[ijpm] + O[n]']
out_deri = ['Output\nDerivatives', 'Output\nDerivatives']
preds = ['Element\nCharacteristic\nPredictions', 'Element\nCharacteristic\nPredictions']

nd_cap = ['Node\nCapacity', 'U[n]']
nd_demand = ['Node\nDemand', 'S[np]']
nd_opex = ['Node\nOPEX', 'V[n]']
nd_capex = ['Node\nCAPEX', 'R[n]']
nd_out = ['Node\nOutFlow', 'Node\nOutFlow']
nd = ['Nodes', 'O[n]']

edge_distance = ['Edge\nDistances', 'Maps API']
edge_carbon = ['Edge\nCO2e', 'E[ijm]']
edge_time = ['Edge\nTransport\nTime', 'T[ijm]']
edge_cost = ['Edge\nTransport\nCost', 'C[ijm]']
edjs = ['Edges', 'X[ijpm]']

ntwrk_scenario = ['Network\nScenario', 'Network\nScenario']
cost_baseline = ['Cost\nBaseline', 'w[cost]']
co_two_baseline = ['CO2\nBaseline', 'w[carbon]']
time_baseline = ['Lead Time\nBaseline', 'w[time]']

flows = ['Flows', 'G[cp]']
alphas = ['Processes', 'alpha[ijp]']

exp = ['conceptual', 'implemented']

#### edges

def draw_master_plan(i, exp):

    G = gv.Digraph('MasterPlan_{}'.format(exp[i]), node_attr={'color': 'lightblue2', 'style': 'filled', 'shape': 'circle'}, engine='dot')

    G.attr(size='6,6', rankdir='LR')

    ## into nodes
    G.edge(ntwrk_struct[i], nd_demand[i], label='historical')
    G.edge(ntwrk_struct[i], nd_cap[i], label='outflow')
    G.edge(ntwrk_struct[i], nd_opex[i], label='outflow')
    G.edge(trsfm_costs[i], nd_opex[i])
    G.edge(frcst_dem[i], nd_demand[i])

    G.edge(nd_capex[i], nd[i])
    G.edge(nd_cap[i], nd[i])
    G.edge(nd_demand[i], nd[i])
    G.edge(nd_opex[i], nd[i])

    ## into edges
    G.edge(ntwrk_struct[i], edge_time[i])
    G.edge(ntwrk_struct[i], edge_cost[i])
    G.edge(ntwrk_struct[i], edge_carbon[i])
    G.edge(edge_distance[i], edge_time[i])
    G.edge(edge_distance[i], edge_carbon[i])
    G.edge(co_two[i], edge_carbon[i])

    G.edge(edge_cost[i], edjs[i])
    G.edge(edge_time[i], edjs[i])
    G.edge(edge_carbon[i], edjs[i])


    ## into network structure
    G.edge(raw_data[i], ntwrk_struct[i], label='NRP')

    ## into flows
    G.edge(ntwrk_struct[i], flows[i])

    ## into manuf processes
    G.edge(ntwrk_struct[i], alphas[i])

    ## into network 
    G.edge(gtc[i], ntwrk[i])
    G.edge(flows[i], ntwrk[i])
    G.edge(alphas[i], ntwrk[i])
    G.edge(edjs[i], ntwrk[i])
    G.edge(nd[i], ntwrk[i])

    ## into network scenario
    G.edge(ntwrk[i], ntwrk_scenario[i])
    G.edge(leech[i], ntwrk_scenario[i])
    G.edge(scenario[i], ntwrk_scenario[i], label='func +')
    G.edge(scenario[i], ntwrk_scenario[i], label='func -')
    G.edge(scenario[i], ntwrk_scenario[i], label='func mod')
    G.edge(preds[i], ntwrk_scenario[i])


    ## risk
    G.edge(risk[i], leech[i], label='randomness')

    ## scenario
    G.edge(scenario[i], preds[i], label='ML')
    G.edge(preds[i], scenario[i])

    ## baseline
    G.edge(ntwrk[i], cost_baseline[i])
    G.edge(ntwrk[i], co_two_baseline[i])
    G.edge(ntwrk[i], time_baseline[i])

    ## into milp
    G.edge(ntwrk_scenario[i], milp[i])
    G.edge(cost_baseline[i], milp[i])
    G.edge(co_two_baseline[i], milp[i])
    G.edge(time_baseline[i], milp[i])


    ## into opti state
    G.edge(milp[i], opti_state[i], label='opt.\nflows')
    # G.edge(milp, opti_state, label='opt.\nnodes')

    ## output derivatives
    G.edge(opti_state[i], out_deri[i], label='biz')


    G.view()


if __name__ == '__main__':
    draw_master_plan(1, exp)