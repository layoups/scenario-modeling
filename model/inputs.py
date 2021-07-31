import numpy as np
import pandas as pd

lanes = {}
nodes = {}

customer_lanes = {} 
gateway_lanes = {}
dslc_lanes = {} 
oslc_lanes = {} 
df_lanes = {} 
ghub_lanes = {} 
pcba_lanes = {}

manufacturing_adjacency_list = {}

C = {} # transportation cost
V = {} # transformation cost
E = {} # co2e
T = {} # time

S = {} # supply
U = {} # capacity
alpha = {}