import numpy as np
import pandas as pd

import snowflake as sf
import snowflake.connector
from snowflake.sqlalchemy import URL

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy import Table, Column, Float, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.automap import automap_base 

from env import DB_CONN_PARAMETER_STG, DB_CONN_PARAMETER_WI

## use stg engine, but prefix WI tables with WI


# engine_local = create_engine(DB_CONN_PARAMETER)

stg_engine = create_engine(DB_CONN_PARAMETER_STG)
stg_metadata = MetaData()
stg_metadata.reflect(stg_engine, extend_existing=True) # extend_existing
stg_Base = automap_base(metadata=stg_metadata)
# print('\n', metadata.tables.keys(), '\n')




wi_engine = create_engine(DB_CONN_PARAMETER_WI)
wi_metadata = MetaData()
wi_metadata.reflect(wi_engine, extend_existing=True) # extend_existing
wi_Base = automap_base(metadata=wi_metadata)
# print('\n', metadata.tables.keys(), '\n')




class RawHANA(stg_Base):

    row_id = Column(Integer, primary_key=True, autoincrement=True)

    __tablename__ = 'scdsi_cv_lane_rate_automation_pl'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return "{}: ({}_{}_{}) -> ({}_{}_{}) | <transport_mode: {}, ship_type: {}>".format(
            self.product_family,
            self.ship_from_name, self.ship_from_country, self.ship_from_region_code,
            self.ship_to_name, self.ship_to_country, self.ship_to_region_code,
            self.transport_mode, self.shipment_type)

class Baselines(stg_Base):

    start = Column("START", String)

    __tablename__ = 'scdsi_baselines'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return 'Baseline {}:  {} to {} - {} - {}'.format(
            self.baseline_id, self.start, self.end, self.date, self.description
            )

class ShipRank(stg_Base):

    __tablename__ = 'scdsi_ship_rank'

    def __repr__(self):
        return "ShipRank(ship_type = {}, ship_rank = {})".format(self.ship_rank, self.ship_type)


class Scenarios(stg_Base):

    __tablename__ = 'scdsi_scenarios'

    def __repr__(self):
        return 'Scenario: {} - Baseline: {} - Date: {} - {} '.format(
            self.scenario_id, self.baseline_id, self.date, self.description
        )


class RawLanes(stg_Base):

    __tablename__ = 'scdsi_nrp_raw_data'

    def __repr__(self):
        return "{} - {}: ({}_{}_{}) -> ({}_{}_{}) | ship_type: {} | total_paid: {}, total_shipped: {}>".format(
            self.product_family, self.fiscal_quarter_ship,
            self.ship_from_name, self.ship_from_country, self.ship_from_region_code,
            self.ship_to_name, self.ship_to_country, self.ship_to_region_code,
            self.shipment_type, self.agg_total_amount_paid, self.agg_chargeable_weight_total_amount
            )


class DecomNodes(stg_Base):

    __tablename__ = 'scdsi_decommisioned_nodes'

    def __repr__(self):
        return '({}, {}) | {} - <{}_{}_{}_{}>'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, self.name, self.country, self.region, self.role
            )


class AltNodes(stg_Base):

    __tablename__ = 'scdsi_alternative_nodes'

    def __repr__(self) -> str:
        return '({}, {}) | {} - <{}_{}_{}_{}> replaces <({}_{}_{}_{})> | supply: {}, capacity: {}, opex'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, 
            self.alt_name, self.alt_country, self.alt_region, self.role,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
            )


class Edges(stg_Base):

    __tablename__ = 'scdsi_edges'

    def __repr__(self) -> str:
        return '({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

stg_Base.prepare()

class Omega(wi_Base):

    __tablename__ = 'scdsi_omega'
    
    def __repr__(self) -> str:
        return '{} | Cost: {}, CO2e: {}, Lead Time: {}'.format(
            self.baseline_id, self.cost, self.co2e, self.lead_time
        )


class AltEdges(wi_Base):

    alt_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_ALTERNATIVE_EDGES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})>".format(
            self.scenario_id, self.baseline_id,
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f
        ) 


class DecomEdges(wi_Base):

    decom_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_DECOMMISIONED_EDGES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})>".format(
            self.scenario_id, self.baseline_id,
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f
        ) 


class ScenarioLanes(wi_Base):

    scenario_row_id = Column(Integer, primary_key=True)
    scenario_id = Column(Integer)
    baseline_id = Column(String)
    pdct_fam = Column(String)
    ori_name = Column(String)
    ori_country = Column(String)
    ori_region = Column(String)
    ori_role = Column(String, default='')
    desti_name = Column(String)
    desti_country = Column(String)
    desti_region = Column(String)
    desti_role = Column(String, default='')
    ship_type = Column(String)
    ship_rank = Column(Integer)
    total_weight = Column(Float)
    total_paid = Column(Float)
    alpha = Column(Float)
    total_alpha = Column(Float)
    color = Column(Integer, default=0)
    d = Column(Integer, default=0)
    f = Column(Integer, default=0)
    path = Column(Integer, default=0)
    path_rank = Column(Integer, default=0)
    pflow = Column(Integer, default=0)
    parent_pflow = Column(Integer)
    in_pflow = Column(Integer)

    __tablename__ = "SCDSI_SCENARIO_LANES"
    # __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})>".format(
            self.scenario_id, self.baseline_id,
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f
        )

    def get_successors(self, session):
        stmt = text("""
        select * from "SCDSI_SCENARIO_LANES"
        where d > {} and f < {}
        """).format(self.d, self.f)
        return session.execute(stmt)


class Locations(wi_Base):

    location_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_LOCATIONS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}_{}_{}) | Latitude: {} - Longitude: {}'.format(
            self.name, self.country, self.region, self.lat, self.long
        )

class ScenarioEdges(wi_Base):

    scenario_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_SCENARIO_EDGES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.scenario_id, self.baseline_id,
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

class ScenarioNodes(wi_Base):

    scenario_node_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_SCENARIO_NODES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}_{}) | supply: {}, capacity: {}, opex: {}'.format(
            self.scenario_id, self.baseline_id,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
        )



class Runs(wi_Base):

    run_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_RUNS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | Date: {} | Lambda Cost: {}, Lambda Time: {}, Lambda CO2e: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.date,
            self.lambda_cost, self.lambda_time, self.lambda_co2e
        )

class OptimalFlows(wi_Base):

    opt_flow_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_OPTIMAL_FLOWS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <transport_mode: {}, flow: {}>".format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.transport_mode, self.flow
        )


class OptimalNodes(wi_Base):

    opt_node_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_OPTIMAL_NODES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | ({}_{}_{}_{}) | state: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.name, self.country, self.region, self.role,
            self.state
        )


class Solution(wi_Base):

    solution_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDSI_SOLUTION"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | <Optimal Cost: {}, Optimal Time: {}, Optimal CO2e: {}>'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.optimal_cost, self.optimal_time, self.optimal_co2e
        )


wi_Base.prepare()

if __name__ == '__main__':
    stg_Session = sessionmaker(bind=stg_engine)
    stg_session = stg_Session()

    raw_row = stg_session.query(RawHANA).first()
    print(raw_row, '\n')

    wi_Session = sessionmaker(bind=wi_engine)
    wi_session = wi_Session()

    raw_baseline = stg_session.query(Baselines).first()
    print(raw_baseline)

    lane = wi_session.query(ScenarioLanes).first()
    print(lane)



    # conn_prod = snowflake.connector.connect(
    #             user='SCDS_SCDSI_ETL_SVC',
    #             password='&p5dr#Hm8g',
    #             account='cisco.us-east-1',
    #             warehouse='SCDS_SCDSI_ETL_WH',
    #             database='SCDS_DB',
    #             schema='SCDS_SCDSI_STG',
    #             role = 'SCDS_SCDSI_ETL_ROLE'
    #             )

    # cur = conn_prod.cursor()
    # cur.execute("""
    # select * from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_BASELINES"
    # """)

    # df = cur.fetch_pandas_all()
    # print(df)