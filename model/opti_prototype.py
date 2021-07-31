#!/opt/conda/bin/python
import gurobipy as gp
from gurobipy import GRB
import numpy as np
# randomly generating given data structures C, V R E T lambda omega, alpha S and U
# add lanes as a dict where the lane_name is the key and the value are an array 
# of tuples [(i,j,p),...] Also create adjacency list . here i assume node aa 
#  to be the only  manufnode.
adj={('aa','cc','CN'): [('bb','aa','CN'), ('cc','aa','CN')],
('aa','cc','IND'): [('bb','aa','IND'), ('cc','aa','IND')],
('aa','cc','US'): [('bb','aa','US'), ('cc','aa','US')],
('aa','bb','CN'): [('cc','aa','CN'), ('bb','aa','CN')],
('aa','bb','IND'): [('cc','aa','IND'), ('bb','aa','IND')],
('aa','bb','US'): [('cc','aa','US'), ('bb','aa','US')]
}
lamb ={'cost':0.8, 'lt':0.1,'emission':0.1}
omega ={'cost':200,'lt':2083408,'emission':28974}
C={}
I=['aa','bb','cc']
J=['aa','bb','cc']
P=['CN','IND','US']
M=['ground','air','sea']
N=['aa','bb','cc']
for i in I:
    for j in J:
        for p in P:
            for m in M:
                C[(i,j,p,m)]= np.random.randint(200)
R={}
for n in N:
    R[n] = np.random.randint(38800)
S,V,U={},{},{}
for n in N:
    for p in P:
        V[(n,p)]= np.random.randint(3820)
        S[(n,p)]= np.random.randint(380)
        U[(n,p)]= np.random.randint(3820)
E, T = {},{}
for i in I:
    for j in J:
        for m in M:
            E[(i,j,m)]= np.random.randint(20000)
            T[(i,j,m)]= np.random.randint(10000)
alpha={}
for i in I:
    for j in J:
        for p in P:
            alpha[(i,j,p)]= np.random.uniform(0,1)
Names = ['customer_lanes','dslc_lanes', 'oslc_lanes', 'df_lanes', 'ghub_lanes', 'pcba_lanes']
lanes ={}
for names in Names:
    dummy =[]
    dummy = list(set([(i,j,p) for (i,j,p,m) in C ]))
    lanes[names]= dummy
# this is where the cut and paste begins
# Define dec vars X, O Q and some manipulation of data structure at the begininng
listC,listU,all_i,all_j =[],[],[],[]
for k in C:
    listC.append(list(k))
for r in listC:
    all_i.append(r[0]) 
    all_j.append(r[1])  
all_i= list(set(all_i))
all_j= list(set(all_j))
for k in U:
    listU.append(list(k))
all_i=[]


m = gp.Model('OPT')
X= m.addVars(((r[0],r[1],r[2],r[3]) for r in listC), name="X",vtype=GRB.CONTINUOUS )
O= m.addVars(((r) for r in R), name="O",vtype=GRB.BINARY)
Q= m.addVars(((r[0], r[1]) for r in listU), name="Q",vtype=GRB.BINARY)

#Create objective
obj =0
obj+= lamb['cost']*omega['cost']*(gp.quicksum((C[(i,j,p,m)]+V[(i,p)])*X[(i,j,p,m)] for (i,j,p,m) in X) + gp.quicksum(O[(n)]*R[(n)] for (n) in O))
obj+= lamb['lt']*omega['lt']*gp.quicksum(T[(i,j,m)]*X[(i,j,p,m)] for (i,j,p,m) in X)
obj+= lamb['emission']*omega['emission']*gp.quicksum(E[(i,j,m)]*X[(i,j,p,m)] for (i,j,p,m) in X)
m.setObjective(obj, GRB.MINIMIZE)

#Set constraints
#ensures that all manufacturing nodes in a location are decommissioned when a location is
#decommissioned.
m.addConstrs(Q[(n,p)]<= O[n] for (n,p) in Q )
#ensures that node capacities cannot be exceeded.
#for(j,p) in Q:
    #m.addConstr(sum(X.select('*',j,p,'*')) <= U[(j, p)] * Q[(j, p)])
m.addConstrs(X.sum('*',j,p,'*') <= U[(j, p)] * Q[(j, p)] for (j,p) in Q)

#ensures conservation of flow in arcs and at nodes
for l,v in lanes.items():
    if (l=='customer_lanes'):
        for single_ton in v:
            a,b,c = single_ton
            m.addConstrs(-X.sum('*',b,p,'*')== S[(b,p)] for (j,p) in S if (j==b) )
    else:
        for single_ton in v:
            a,b,c = single_ton
            m.addConstrs(X.sum(b,'*',p,'*')-X.sum('*',b,p,'*')== S[(b,p)] for (j,p) in S if (j==b) )
#ensures manufacturing processes are respected
for key in adj:
    for e in adj[key]:
        m.addConstr(sum(X.select(e[0],e[1],e[2],'*'))== alpha[(e[0], e[1], e[2])] * (sum(X.select(key[0],'*',e[2],'*')) - S[(key[0], e[2])]))

m.write('opt.lp')

m.optimize()
