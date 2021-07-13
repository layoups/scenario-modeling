from env import DB_CONN_PARAMETER
import numpy as np

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker

from AutoMap import *

def get_main_pflow(pdct_fam, session):

    model_pflows = session.query(Lanes.pflow, Lanes.parent_pflow, Lanes.pdct_fam, Lanes.in_pflow). \
        filter(Lanes.parent_pflow == None , Lanes.pdct_fam == pdct_fam, Lanes.in_pflow == 1). \
            distinct()
    return model_pflows

def get_customer_alphas(pdct_fam, session):
    customers = session.query(Nodes).filter(Nodes.role == 'Customer', Nodes.pdct_fam == pdct_fam)
    for c in customers:
        c_pflow = session.query(Lanes).filter(
            Lanes.desti_name == c.name,
            Lanes.desti_country == c.country, 
            Lanes.desti_region == c.region, 
            Lanes.desti_role == c.role,
            Lanes.pdct_fam == c.pdct_fam,
            Lanes.in_pflow == 1
        )
        total = np.sum([e.total_weight for e in c_pflow.all()])
        for e in c_pflow.all():
            e.alpha = round(e.total_weight / total)
            e.total_alpha = e.alpha
        session.commit()


def get_alphas(pdct_fam, session):
    model_pflows = get_main_pflow(pdct_fam, session)
    for p in model_pflows.all():
        graph = session.query(Lanes).filter(
            Lanes.pdct_fam == pdct_fam, 
            Lanes.pflow == p.pflow
            ).order_by(
                Lanes.path, 
                desc(Lanes.ship_rank)
            )
        for edge in graph.all():
            adj = session.query(Lanes).filter(
                Lanes.pdct_fam == pdct_fam, 
                Lanes.d > edge.d, 
                Lanes.f < edge.f, 
                Lanes.path_rank == edge.path_rank + 1, 
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

    get_customer_alphas('4400ISR' ,session)
    get_alphas('4400ISR', session)