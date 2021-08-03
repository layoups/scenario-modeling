from env import DB_CONN_PARAMETER_WI
import numpy as np

from sqlalchemy import create_engine, desc, distinct, or_
from sqlalchemy.orm import sessionmaker

from AutoMap import *

def get_main_pflow(scenario_id, baseline_id, pdct_fam, session):
    model_pflows = session.query(ScenarioLanes.pflow).filter(
        ScenarioLanes.scenario_id == scenario_id,
        ScenarioLanes.baseline_id == baseline_id,
        ScenarioLanes.parent_pflow == None,
        ScenarioLanes.pdct_fam == pdct_fam,
        ScenarioLanes.in_pflow == 1
    ).distinct()
    return model_pflows.all()

def get_customer_alphas(scenario_id, baseline_id, pdct_fam, session):
    customers = session.query(
        ScenarioLanes.desti_name, 
        # ScenarioLanes.desti_country, 
        ScenarioLanes.desti_region,
        ScenarioLanes.desti_role
        ).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.desti_role == 'Customer', 
            ScenarioLanes.pdct_fam == pdct_fam,
            ScenarioLanes.in_pflow == 1
        ).distinct().all()
    for c in customers:
        c_pflow = session.query(ScenarioLanes).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.desti_name == c.desti_name,
            # ScenarioLanes.desti_country == c.desti_country, 
            ScenarioLanes.desti_region == c.desti_region, 
            ScenarioLanes.desti_role == c.desti_role,
            ScenarioLanes.pdct_fam == pdct_fam,
            ScenarioLanes.in_pflow == 1
        )
        c_pflow = c_pflow.all()
        total = np.sum([e.total_weight for e in c_pflow])
        alpha = 0
        for e in c_pflow[:-1]:
            e.alpha = round(e.total_weight / total, 5)
            e.total_alpha = e.alpha
            alpha += e.alpha
        if len(c_pflow) > 0:
            c_pflow[-1].alpha = 1 - alpha
            c_pflow[-1].total_alpha = c_pflow[-1].alpha
        session.commit()


def get_alphas(scenario_id, baseline_id, pdct_fam, session):
    model_pflows = get_main_pflow(scenario_id, baseline_id, pdct_fam, session)
    for p in model_pflows:
        graph = session.query(ScenarioLanes).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.pdct_fam == pdct_fam, 
            ScenarioLanes.in_pflow == 1,
            or_(ScenarioLanes.pflow == p.pflow, ScenarioLanes.parent_pflow == p.pflow)
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
                ScenarioLanes.in_pflow == 1
            )
            adj = adj.all()
            total = np.sum([e.total_weight for e in adj])
            alpha = 0
            for e in adj[:-1]:
                e.alpha = round(e.total_weight / total, 5)
                e.total_alpha = round(e.alpha * edge.total_alpha, 5)
                alpha += e.alpha
            if len(adj) > 0:
                adj[-1].alpha = 1 - alpha
                adj[-1].total_alpha = round(adj[-1].alpha * edge.total_alpha, 5)
        session.commit()

if __name__ == "__main__":
    engine = create_engine(DB_CONN_PARAMETER_CLOUD)
    Session = sessionmaker(bind=engine)
    session = Session()

    baseline_id = 4
    scenario_id = 0

    pdct_fams = [('AIRANT',), ('C2960X',), ('4400ISR',), ('WPHONE',), ('SBPHONE',), ('PHONE',)]

    for pdct_fam in pdct_fams:
        print(pdct_fam)
        get_customer_alphas(scenario_id, baseline_id, pdct_fam[0], session)
        get_alphas(scenario_id, baseline_id, pdct_fam[0], session)