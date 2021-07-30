import gurobipy as GRB

from inputs import *

def run_model(objective_weights):
    model = GRB.Model()
    model.params.outputFlag = 1

    X = model.addVars(
        [(i, j, p, m) for i, j, p, m in lanes],
        vtype=GRB.CONTINUOUS,
        lb=0.0,
        name="X",
    )

    O = model.addVars(
        [(n, p) for n, p in nodes],
        vtype=GRB.BINARY,
        name="O",
    )



