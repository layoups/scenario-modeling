# Model Implementation

## Objectives
    - Data Dictionaries: C[(ijpm)], V[(ip)], E[(ijm)], T[(ijm)]
    - Necessary Indices: i, j, p, m
    - Decision Vars Indices: i, j, p, m - scenario_lanes
    - Generate: for lane in scenario_lanes, X = model.addVars((i, j, p, m))


## Constraints

#### Flow Constraint
    - Data Dictionaries: S[(jp)]
    - Necessary Indices: j, p
    - Decision Vars Indices: *, j, p, * - j, *, p, *
    - Considerations: 
        - No outflow from customer nodes j -- must set aside X where j is a customer 
        - No inflow into supplier nodes i -- must set aside X where i is a supplier
        - Make customer_lanes, dslc_lanes, oslc_lanes, df_lanes, ghub_lanes, where j is of role <role>_lanes
