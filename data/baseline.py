import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker

from collections import deque

from datetime import datetime

from AutoMap import *
from PathRoles import *
from Eraser import *
from Visualize import *
from Alpha import *
from dfs import *

def create_baseline(baseline_id, start, end, description, session):
    date = datetime.now()

    baseline = Baselines(baseline_id=baseline_id)
    baseline.start = start
    baseline.end = end
    baseline.description = description
    baseline.date = date
    session.add(baseline)

    scenario = Scenarios(scenario_id=0)
    scenario.baseline_id = baseline_id
    scenario.date = date
    scenario.description = description
    session.add(scenario)

    session.commit()


def populate_scenario_lanes(baseline_id, session):
    baseline = session.query(Baselines).filter(Baselines.baseline_id == baseline_id).first()

    stmt = text("""
        insert into "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" (scenario_row_id, scenario_id, baseline_id, pdct_fam, ori_name, ori_country, ori_region, desti_name, desti_country, desti_region, ship_type, ship_rank, total_weight, total_paid)
        select "ROW_ID", 0, :baseline_id, "PRODUCT_FAMILY", 
        lower("SHIP_FROM_NAME"), lower("SHIP_FROM_COUNTRY"), lower("SHIP_FROM_REGION_CODE"),
        lower("SHIP_TO_NAME"), lower("SHIP_TO_COUNTRY"), lower("SHIP_TO_REGION_CODE"),
        "SHIPMENT_TYPE", ship_rank,
        sum("BILLED_WEIGHT"), sum("TOTAL_AMOUNT_PAID")
        from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_CV_LANE_RATE_AUTOMATION_PL" rl join "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SHIP_RANK" sr 
        on rl."SHIPMENT_TYPE" = sr.ship_type
        where "BILLED_WEIGHT" != 0 
        and "SHIPMENT_TYPE" not in ('OTHER', 'BROKERAGE')
        and "SHIP_DATE_PURE_SHIP" >= :start and "SHIP_DATE_PURE_SHIP" <= :end
        and "PRODUCT_FAMILY" not in ('TBA')
        group by "ROW_ID", "PRODUCT_FAMILY", 
        "SHIP_FROM_NAME", "SHIP_FROM_COUNTRY", "SHIP_FROM_REGION_CODE", 
        "SHIP_TO_NAME", "SHIP_TO_COUNTRY", "SHIP_TO_REGION_CODE", 
        "SHIPMENT_TYPE", ship_rank;
    """).params(baseline_id = baseline_id, start = baseline.start, end = baseline.end)

    session.execute(stmt)
    session.commit()


    if __name__ == '__main__':
        Session = sessionmaker(bind=engine)

        baseline_id = "'nam"
        create_baseline(baseline_id, '2020-01-01', '2020-12-31', 'trial', Session())
        populate_scenario_lanes(baseline_id, Session())