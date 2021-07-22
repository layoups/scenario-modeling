# Flow from Raw to Run
- insert into Locations select distinct locations from RawHana
- for each location in Locations get lat long from google API

## Baseline
- create baseline: start, end
- create scenario 0
- insert into ScenarioEdges select origin, destination, mode, agg total_weight, agg total_paid, agg total_paid/ agg total_paid as cost from RawHana (Fid = baseline, 0)
- update ScenarioEdges set distances, transport_time, co2e (Fid = baseline, 0)
- insert dfs columns into ScenarioLanes select from RawHANA from start to end (Fid = baseline, 0)
- Run dfs
- 