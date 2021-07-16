from gurobipy import *

m = Model()
v0 = m.addVar()
v1 = m.addVar()
m.update()
m.addConstr(v0 - v1 <= 4)
m.addConstr(v0 + v1 <= 4)
m.addConstr(-0.25*v0 + v1 <= 1)
m.setObjective(v1, GRB.MAXIMIZE)
m.params.outputflag = 0
m.optimize()

import matplotlib.pyplot as pyplot
pyplot.plot([0,4], [0,4])
pyplot.plot([4,0], [0,4])
pyplot.plot([0,4], [1,2])
pyplot.plot([v0.x], [v1.x], 'ro')
pyplot.show()