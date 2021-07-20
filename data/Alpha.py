from env import DB_CONN_PARAMETER
import numpy as np

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker

from AutoMap import *

def get_main_pflow(scenario_id, baseline_id, pdct_fam, session):

    model_pflows = session.query(ScenarioLanes.pflow, ScenarioLanes.parent_pflow, ScenarioLanes.pdct_fam, ScenarioLanes.in_pflow). \
        filter(
            ScenarioNodes.scenario_id == scenario_id,
            ScenarioNodes.baseline_id == baseline_id,
            ScenarioLanes.parent_pflow == None , 
            ScenarioLanes.pdct_fam == pdct_fam, 
            ScenarioLanes.in_pflow == 1). \
            distinct()
    return model_pflows

def get_customer_alphas(scenario_id, baseline_id, pdct_fam, session):
    customers = session.query(ScenarioNodes).filter(
        ScenarioNodes.scenario_id == scenario_id,
        ScenarioNodes.baseline_id == baseline_id,
        ScenarioNodes.role == 'Customer', 
        ScenarioNodes.pdct_fam == pdct_fam
    )
    for c in customers:
        c_pflow = session.query(ScenarioLanes).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.desti_name == c.name,
            ScenarioLanes.desti_country == c.country, 
            ScenarioLanes.desti_region == c.region, 
            ScenarioLanes.desti_role == c.role,
            ScenarioLanes.pdct_fam == c.pdct_fam,
            ScenarioLanes.in_pflow == 1
        )
        total = np.sum([e.total_weight for e in c_pflow.all()])
        for e in c_pflow.all():
            e.alpha = round(e.total_weight / total)
            e.total_alpha = e.alpha
        session.commit()


def get_alphas(scenario_id, baseline_id, pdct_fam, session):
    model_pflows = get_main_pflow(scenario_id, baseline_id, pdct_fam, session)
    for p in model_pflows.all():
        graph = session.query(ScenarioLanes).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.pdct_fam == pdct_fam, 
            ScenarioLanes.pflow == p.pflow
            ).order_by(
                ScenarioLanes.path, 
                desc(ScenarioLanes.ship_rank)
            )
        for edge in graph.all():
            adj = session.query(ScenarioLanes).filter(
                ScenarioLanes.scenario_id == scenario_id,
                ScenarioLanes.baseline_id == baseline_id,
                ScenarioLanes.pdct_fam == pdct_fam, 
                ScenarioLanes.d > edge.d, 
                ScenarioLanes.f < edge.f, 
                ScenarioLanes.path_rank == edge.path_rank + 1, 
            )
            total = np.sum([e.total_weight for e in adj.all()])
            for e in adj.all():
                e.alpha = round(e.total_weight / total, 5)
                e.total_alpha = round(e.alpha * edge.total_alpha, 5)
        session.commit()

if __name__ == "__main__":
    engine = create_engine(DB_CONN_PARAMETER)
    Session = sessionmaker(bind=engine)
    session = Session()

    get_customer_alphas(0, 1, '4400ISR' ,session)
    get_alphas(0, 1, '4400ISR', session)