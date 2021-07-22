# Flow from Raw to Run
- insert into Locations select distinct locations from RawHana
- for each location in Locations get lat long from google API

## Baseline
- create baseline: start, end
- create scenario 0
- insert into ScenarioEdges select origin, destination, mode, agg total_weight, agg total_paid, agg total_paid/ agg total_paid as cost from RawHana (Fid = baseline, 0)
- update ScenarioEdges set distances, transport_time, co2e (Fid = baseline, 0)
- insert dfs columns into ScenarioLanes select from RawHANA from start to end (Fid = baseline, 0)
- run dfs
- insert into ScenarioNodes select nodes from dfs, set in_pflow = 1 (Fid = baseline, 0)
- update ScenarioNodes set node demand
- update ScenarioNodes set node capacity
- update ScenarioNodes set node opex
- update ScenarioEdges set in_pflow = 1 if corresponding edge in ScenarioLanes is in_pflow
- insert into omega baselines for cost, co2e, leadtime


## Scenario > 0
- copy ScenarioLanes where scenario_id = 0, changing scenario_id
- copy ScenarioNodes where scenario_id = 0, changing scenario_id
- copy ScenarioEdges where scenario_id = 0, changing scenario_id
- add row to AltNodes
- add row to DecomNodes
- add row to AltEdges
- add row to DecomEdges
- AltNodes to AltEdges
- DecomNodes to DecomEdges
- AltEdges to ScenarioEdges
- DecomEdges to ScenarioEdges
- AltEdges to ScenarioLanes
- DecomEdges to ScenarioLanes

## Run
- insert into Runs lambdas