from enum import auto
import numpy as np
import pandas as pd

import snowflake as sf
import snowflake.connector
from snowflake.sqlalchemy import URL

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy import Table, Column, Float, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.automap import automap_base 

from env import DB_CONN_PARAMETER

## use stg engine, but prefix WI tables with WI


# engine_local = create_engine(DB_CONN_PARAMETER)

engine = create_engine(DB_CONN_PARAMETER)

metadata = MetaData()
metadata.reflect(engine, extend_existing=True) # extend_existing

# print('\n', metadata.tables.keys(), '\n')


auto_Base = automap_base(metadata=metadata)

class RawHANA(auto_Base):

    row_id = Column(Integer, primary_key=True, autoincrement=True)

    __tablename__ = 'scdsi_cv_lane_rate_automation_pl'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return "{}: ({}_{}_{}) -> ({}_{}_{}) | <transport_mode: {}, ship_type: {}>".format(
            self.product_family,
            self.ship_from_name, self.ship_from_country, self.ship_from_region_code,
            self.ship_to_name, self.ship_to_country, self.ship_to_region_code,
            self.transport_mode, self.shipment_type)

class Baselines(auto_Base):

    start = Column("START", String)

    __tablename__ = 'scdsi_baselines'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return 'Baseline {}:  {} to {} - {} - {}'.format(
            self.baseline_id, self.start, self.end, self.date, self.description
            )

class ShipRank(auto_Base):

    __tablename__ = 'scdsi_ship_rank'

    def __repr__(self):
        return "ShipRank(ship_type = {}, ship_rank = {})".format(self.ship_rank, self.ship_type)


class Scenarios(auto_Base):

    __tablename__ = 'scdsi_scenarios'

    def __repr__(self):
        return 'Scenario: {} - Baseline: {} - Date: {} - {} '.format(
            self.scenario_id, self.baseline_id, self.date, self.description
        )


class RawLanes(auto_Base):

    __tablename__ = 'scdsi_nrp_raw_data'

    def __repr__(self):
        return "{} - {}: ({}_{}_{}) -> ({}_{}_{}) | ship_type: {} | total_paid: {}, total_shipped: {}>".format(
            self.product_family, self.fiscal_quarter_ship,
            self.ship_from_name, self.ship_from_country, self.ship_from_region_code,
            self.ship_to_name, self.ship_to_country, self.ship_to_region_code,
            self.shipment_type, self.agg_total_amount_paid, self.agg_chargeable_weight_total_amount
            )


class DecomNodes(auto_Base):

    __tablename__ = 'scdsi_decommisioned_nodes'

    def __repr__(self):
        return '({}, {}) | {} - <{}_{}_{}_{}>'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, self.name, self.country, self.region, self.role
            )


class AltNodes(auto_Base):

    __tablename__ = 'scdsi_alternative_nodes'

    def __repr__(self) -> str:
        return '({}, {}) | {} - <{}_{}_{}_{}> replaces <({}_{}_{}_{})> | supply: {}, capacity: {}, opex'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, 
            self.alt_name, self.alt_country, self.alt_region, self.role,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
            )


class Edges(auto_Base):

    __tablename__ = 'scdsi_edges'

    def __repr__(self) -> str:
        return '({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

class Omega(auto_Base):

    baseline_id = Column(Integer, primary_key=True)

    __tablename__ = 'scds_scdsi_wi.scdsi_omega'
    
    def __repr__(self) -> str:
        return '{} | Cost: {}, CO2e: {}, Lead Time: {}'.format(
            self.baseline_id, self.cost, self.co2e, self.lead_time
        )


class AltEdges(auto_Base):

    alt_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_ALTERNATIVE_EDGES"
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


class DecomEdges(auto_Base):

    decom_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_DECOMMISIONED_EDGES"
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


class ScenarioLanes(auto_Base):

    scenario_row_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_SCENARIO_LANES"
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

    def get_successors(self, session):
        stmt = text("""
        select * from "SCDS_SCDSI_WI.SCDSI_SCENARIO_LANES"
        where d > {} and f < {}
        """).format(self.d, self.f)
        return session.execute(stmt)


class Locations(auto_Base):

    location_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_LOCATIONS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}_{}_{}) | Latitude: {} - Longitude: {}'.format(
            self.name, self.country, self.region, self.lat, self.long
        )

class ScenarioEdges(auto_Base):

    scenario_edge_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_SCENARIO_EDGES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.scenario_id, self.baseline_id,
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

class ScenarioNodes(auto_Base):

    scenario_node_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_SCENARIO_NODES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}_{}) | supply: {}, capacity: {}, opex: {}'.format(
            self.scenario_id, self.baseline_id,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
        )



class Runs(auto_Base):

    run_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_RUNS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | Date: {} | Lambda Cost: {}, Lambda Time: {}, Lambda CO2e: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.date,
            self.lambda_cost, self.lambda_time, self.lambda_co2e
        )

class OptimalFlows(auto_Base):

    opt_flow_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_OPTIMAL_FLOWS"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <transport_mode: {}, flow: {}>".format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.transport_mode, self.flow
        )


class OptimalNodes(auto_Base):

    opt_node_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_OPTIMAL_NODES"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | ({}_{}_{}_{}) | state: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.name, self.country, self.region, self.role,
            self.state
        )


class Solution(auto_Base):

    solution_id = Column(Integer, primary_key=True)

    __tablename__ = "SCDS_SCDSI_WI.SCDSI_SOLUTION"
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | <Optimal Cost: {}, Optimal Time: {}, Optimal CO2e: {}>'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.optimal_cost, self.optimal_time, self.optimal_co2e
        )


auto_Base.prepare()

if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()

    raw_row = session.query(RawHANA).first()
    print(raw_row, '\n')

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