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
        - Make customer_lanes, dslc_lanes, oslc_lanes, df_lanes, ghub_lanes, where j in (i, j, p, m) is of role <role>_lanes
    - Generate: 
        - for x in customer_lanes, -X.sum(*, j, p, *) == S[(j, p)]
        - for x in dslc_lanes/oslc_lanes/df_lanes/ghub_lanes, X.sum(j, *, p, *) - X.sum(*, j, p, *) == S[(j, p)]

    
#### Capacity Constraint
    - Data Dictionary: U[(j, p)]
    - Necessary Indices: j, p

