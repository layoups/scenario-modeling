# Model Implementation

## Objectives
    - Data Dictionaries: C[(i, j, p, m)], V[(i, p)], E[(i, j, m)], T[(i, j, m)]
    - Necessary Indices: i, j, p, m
    - Decision Vars Indices: i, j, p, m - scenario_lanes

    - Generate: for lane in scenario_lanes, X = model.addVars((i, j, p, m))


## Constraints

#### Flow Constraint
    - Data Dictionaries: S[(j, p)]
    - Necessary Indices: j, p
    - Decision Vars Indices: *, j, p, * - j, *, p, *
    - Considerations: 
        - No outflow from customer nodes j -- must set aside X where j is a customer 
        - No inflow into supplier nodes i -- must set aside X where i is a supplier
        - Make customer_lanes, dslc_lanes, oslc_lanes, df_lanes, ghub_lanes, pcba_lanes where j in (i, j, p) is of role <role>_lanes = [(i, j, p)]
    - Generate: 
        - for x in customer_lanes, -X.sum(*, j, p, *) == S[(j, p)]
        - for x in dslc_lanes/oslc_lanes/df_lanes/ghub_lanes/pcba_lanes, X.sum(j, *, p, *) - X.sum(*, j, p, *) == S[(j, p)]

    
#### Capacity Constraint
    - Data Dictionaries: U[(j, p)]
    - Necessary Indices: j, p
    - Decision Var Indices: *, j, p, *
    - Considerations:
        - Only warehousing and manufacturing nodes have capacities
    - Generate:
        - for x in dslc_lanes/oslc_lanes/df_lanes/ghub_lanes/pcba_lanes, X.sum(*, j, p, *) <= U[(j, p)] * Q[j, p]


#### Alpha Constraint
    - Data Dictionaries: alpha[(i, j, p)], S[(j, p)]
    - Necessary Indices: i, j, p
    - Decision Var Indices: i, j, p, *
    - Considerations:
        - Manufacturing processes must only be respected at manufacturing nodes (DF, PCBA)
        - Adjacency list is necessary to accomodate alternative nodes
            - manufacturing_adjency_list = {(j, k, p): [
                                                            [(i, j, p), (i', j, p), (i'', j, p)],
                                                            [(a, j, p)],
                                                            [(l, j, p), (l', j, p)]
                                                        ]
                                            }, where j is a manufacturing nodes
    - Generate:
        - for x in manufacturing_adjacency_list: 
            adj = manufacturing_adjacency_list[x]
            for y in adj:
                model.addConstr(
                    np.sum([X.sum(z[0], z[1], z[2], *) for z in y]) 
                    == alpha[(z[0], z[1], z[2])] * (X.sum(x[0], *, p, *) - S[(x[0], p)]
                    )


X.sum(hk, tx, p, *) == alpha[(hk, tx)] * (X.sum(tx, *, p, *) - S[(tx, p)])
X.sum(hk, tx, p, *) == alpha[(hk, tx)] * (X.sum(tx, *, p, *) - 0)
