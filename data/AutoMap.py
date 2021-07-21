from decimal import FloatOperation
import numpy as np
import pandas as pd

import snowflake as sf
import snowflake.connector
from snowflake.sqlalchemy import URL

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy import Table, Column, Float, String, Integer, ForeignKey, Date
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql.expression import null


from env import DB_CONN_PARAMETER_STG, DB_CONN_PARAMETER_WI

## use stg engine, but prefix WI tables with WI


# engine_local = create_engine(DB_CONN_PARAMETER)

# stg_engine = create_engine(DB_CONN_PARAMETER_STG)
# stg_metadata = MetaData()
# stg_metadata.reflect(stg_engine, extend_existing=True) # extend_existing
# stg_Base = automap_base(metadata=stg_metadata)
# print('\n', metadata.tables.keys(), '\n')




engine = create_engine(DB_CONN_PARAMETER_WI)
metadata = MetaData()
metadata.reflect(engine) # extend_existing
Base = automap_base(metadata=metadata)
# print('\n', metadata.tables.keys(), '\n')




class RawHANA(Base):

    row_id = Column(Integer, primary_key=True, autoincrement=True)

    __tablename__ = 'scds_db.scds_scdsi_stg.scdsi_cv_lane_rate_automation_pl'


    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return "{}: ({}_{}_{}) -> ({}_{}_{}) | <transport_mode: {}, ship_type: {}>".format(
            self.product_family,
            self.ship_from_name, self.ship_from_country, self.ship_from_region_code,
            self.ship_to_name, self.ship_to_country, self.ship_to_region_code,
            self.transport_mode, self.shipment_type)


class Baselines(Base):
    
    baseline_id = Column('baseline_id', String, primary_key=True, nullable=True)
    date = Column('date', Date)
    description = Column('description', String)
    start = Column("START", String)
    end = Column('end', String)

    __tablename__ = 'scdsi_baselines'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return 'Baseline {}:  {} to {} - {} - {}'.format(
            self.baseline_id, self.start, self.end, self.date, self.description
            )

class ShipRank(Base):

    ship_type_id = Column('ship_type_id', primary_key=True, autoincrement=True)
    ship_type = Column('ship_type', String)
    ship_rank = Column('ship_rank', Integer)

    __tablename__ = 'scdsi_ship_rank'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return "ShipRank(ship_type = {}, ship_rank = {})".format(self.ship_rank, self.ship_type)


class Scenarios(Base):

    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'), primary_key=True)
    scenario_id = Column('scenario_id', Integer, primary_key=True, nullable=True)
    date = Column('date', Date)
    description = Column('description', String)

    __tablename__ = 'scdsi_scenarios'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return 'Scenario: {} - Baseline: {} - Date: {} - {} '.format(
            self.scenario_id, self.baseline_id, self.date, self.description
        )


class RawLanes(Base):

    id = Column('id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    pdct_fam = Column('pdct_fam', String)
    shipment_type = Column('shipment_type', String)
    fiscal_quarter_ship = Column('fiscal_quarter_ship', String)
    agg_total_amount_paid = Column('agg_total_amount_paid', Float)
    agg_chargeable_weight_total_amount = Column('agg_chargeable_weight_total_amount', Float)
    ori_name = Column('ori_name', String)
    ori_country = Column('ori_country', String)
    ori_region = Column('ori_region', String)
    desti_name = Column('desti_name', String)
    desti_country = Column('desti_country', String)
    desti_region = Column('desti_region', String)

    __tablename__ = 'scdsi_nrp_raw_data'
    __table_args__ = {'extend_existing': True}


    def __repr__(self):
        return "{} | {} - {}: ({}_{}_{}) -> ({}_{}_{}) | ship_type: {} | total_paid: {}, total_shipped: {}>".format(
            self.baseline_id, 
            self.pdct_fam, self.fiscal_quarter_ship,
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.shipment_type, self.agg_total_amount_paid, self.agg_chargeable_weight_total_amount
            )


class DecomNodes(Base):

    decom_node_id = Column('decom_node_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    country = Column('country', String)
    region = Column('region', String)

    __tablename__ = 'scdsi_decommisioned_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}, {}) | {} - <{}_{}_{}_{}>'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, self.name, self.country, self.region, self.role
            )


class AltNodes(Base):

    alt_node_id = Column('alt_node_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    country = Column('country', String)
    region = Column('region', String)
    alt_name = Column('alt_name', String)
    alt_country = Column('alt_country', String)
    alt_region = Column('alt_region', String)
    supply = Column('supply', Float)
    capacity = Column('capacity', Float)
    opex = Column('opex', Float)

    __tablename__ = 'scdsi_alternative_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | {} - <{}_{}_{}_{}> replaces <({}_{}_{}_{})> | supply: {}, capacity: {}, opex'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, 
            self.alt_name, self.alt_country, self.alt_region, self.role,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
            )


class Edges(Base):

    edge_id = Column('edge_id', Integer, primary_key=True, nullable=True)
    distance = Column('distance', Float)
    co2e = Column('co2e', Float)
    transport_mode = Column('transport_mode', String)
    transport_time = Column('transport_time', Float)
    ori_name = Column('ori_name', String)
    ori_country = Column('ori_country', String)
    ori_region = Column('ori_region', String)
    desti_name = Column('desti_name', String)
    desti_country = Column('desti_country', String)
    desti_region = Column('desti_region', String)


    __tablename__ = 'scdsi_edges'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )
class Omega(Base):

    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'), primary_key=True, nullable=True)
    baseline_cost = Column('baseline_cost', Float)
    baseline_lead_time = Column('baseline_lead_time', Float)
    baseline_co2e = Column('baseline_co2e', Float)


    __tablename__ = 'scdsi_omega'
    __table_args__ = {'extend_existing': True}
    
    def __repr__(self) -> str:
        return '{} | Cost: {}, CO2e: {}, Lead Time: {}'.format(
            self.baseline_id, self.cost, self.co2e, self.lead_time
        )


class AltEdges(Base):

    alt_edge_id = Column(Integer, primary_key=True, nullable=True)


    __tablename__ = 'scdsi_alternative_edges'
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


class DecomEdges(Base):

    decom_edge_id = Column('decom_edge_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_country = Column('ori_country', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
    desti_country = Column('desti_country', String)
    desti_region = Column('desti_region', String)
    desti_role = Column('desti_role', String, default='')
    ship_type = Column('ship_type', String)
    ship_rank = Column('ship_rank', Integer)
    total_weight = Column('total_weight', Float)
    total_paid = Column('total_paid', Float)
    alpha = Column('alpha', Float)
    total_alpha = Column('total_alpha', Float)
    color = Column('color', Integer, default=0)
    d = Column('d', Integer, default=0)
    f = Column('f', Integer, default=0)
    path = Column('path', Integer, default=0)
    path_rank = Column('path_rank', Integer, default=0)
    pflow = Column('pflow', Integer, default=0)
    parent_pflow = Column('parent_pflow', Integer)
    in_pflow = Column('in_pflow', Integer)

    __tablename__ = 'scdsi_decommisioned_edges'
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


class ScenarioLanes(Base):

    scenario_row_id = Column('scenario_row_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_country = Column('ori_country', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
    desti_country = Column('desti_country', String)
    desti_region = Column('desti_region', String)
    desti_role = Column('desti_role', String, default='')
    ship_type = Column('ship_type', String)
    ship_rank = Column('ship_rank', Integer)
    total_weight = Column('total_weight', Float)
    total_paid = Column('total_paid', Float)
    alpha = Column('alpha', Float)
    total_alpha = Column('total_alpha', Float)
    color = Column('color', Integer, default=0)
    d = Column('d', Integer, default=0)
    f = Column('f', Integer, default=0)
    path = Column('path', Integer, default=0)
    path_rank = Column('path_rank', Integer, default=0)
    pflow = Column('pflow', Integer, default=0)
    parent_pflow = Column('parent_pflow', Integer)
    in_pflow = Column('in_pflow', Integer)

    __tablename__ = 'scdsi_scenario_lanes'
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
        select * from "SCDSI_SCENARIO_LANES"
        where d > {} and f < {}
        """).format(self.d, self.f)
        return session.execute(stmt)


class Locations(Base):

    location_id = Column(Integer, primary_key=True)
    name = Column(String)
    country = Column(String)
    region = Column(String)
    lat = Column(Float)
    long = Column(Float)

    __tablename__ = 'scdsi_locations'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}_{}_{}) | Latitude: {} - Longitude: {}'.format(
            self.name, self.country, self.region, self.lat, self.long
        )

class ScenarioEdges(Base):

    scenario_edge_id = Column(Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_country = Column('ori_country', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
    desti_country = Column('desti_country', String)
    desti_region = Column('desti_region', String)
    desti_role = Column('desti_role', String, default='')
    transport_mode = Column('transport_mode', String)
    transport_time = Column('transport_time', Float)
    co2e = Column('co2e', Float)
    transport_cost = Column('transport_cost', Float)
    distance = Column('distance', Float)

    __tablename__ = 'scdsi_scenario_edges'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.scenario_id, self.baseline_id,
            self.ori_name, self.ori_country, self.ori_region,
            self.desti_name, self.desti_country, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

class ScenarioNodes(Base):

    scenario_node_id = Column(Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    country = Column('country', String)
    region = Column('region', String)
    supply = Column('supply', Float)
    capacity = Column('capacity', Float)
    opex = Column('opex', Float)

    __tablename__ = 'scdsi_scenario_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}_{}) | supply: {}, capacity: {}, opex: {}'.format(
            self.scenario_id, self.baseline_id,
            self.name, self.country, self.region, self.role,
            self.supply, self.capacity, self.opex
        )



class Runs(Base):

    run_id = Column(Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'), primary_key=True, nullable=True)
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'), primary_key=True, nullable=True)
    date = Column('date', Date)
    description = Column('description', String)
    lambda_cost = Column('lambda_cost', Float)
    lambda_time = Column('lambda_time', Float)
    lambda_co2e = Column('lambda_co2e', Float)

    __tablename__ = 'scdsi_runs'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | Date: {} | Lambda Cost: {}, Lambda Time: {}, Lambda CO2e: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.date,
            self.lambda_cost, self.lambda_time, self.lambda_co2e
        )

class OptimalFlows(Base):

    opt_flow_id = Column(Integer, primary_key=True)

    __tablename__ = 'scdsi_optimal_flows'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <transport_mode: {}, flow: {}>".format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.transport_mode, self.flow
        )


class OptimalNodes(Base):

    opt_node_id = Column(Integer, primary_key=True)

    __tablename__ = 'scdsi_optimal_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | ({}_{}_{}_{}) | state: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.name, self.country, self.region, self.role,
            self.state
        )


class Solution(Base):

    solution_id = Column(Integer, primary_key=True)

    __tablename__ = 'scdsi_solution'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | <Optimal Cost: {}, Optimal Time: {}, Optimal CO2e: {}>'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.optimal_cost, self.optimal_time, self.optimal_co2e
        )


Base.prepare()

def test_auto_mapping():
    print(RawHANA.__tablename__, RawHANA.__table__.columns.keys())
    print(Baselines.__tablename__, Baselines.__table__.columns.keys())
    print(ShipRank.__tablename__, ShipRank.__table__.columns.keys())
    print(Scenarios.__tablename__, Scenarios.__table__.columns.keys())
    print(RawLanes.__tablename__, RawLanes.__table__.columns.keys())
    print(DecomNodes.__tablename__, DecomNodes.__table__.columns.keys())
    print(AltNodes.__tablename__, AltNodes.__table__.columns.keys())
    print(Edges.__tablename__, Edges.__table__.columns.keys())
    print(Omega.__tablename__, Omega.__table__.columns.keys())
    print(AltEdges.__tablename__, AltEdges.__table__.columns.keys())
    print(DecomEdges.__tablename__, DecomEdges.__table__.columns.keys())
    print(ScenarioLanes.__tablename__, ScenarioLanes.__table__.columns.keys())
    print(Locations.__tablename__, Locations.__table__.columns.keys())
    print(ScenarioEdges.__tablename__, ScenarioEdges.__table__.columns.keys())
    print(ScenarioNodes.__tablename__, ScenarioNodes.__table__.columns.keys())
    print(Runs.__tablename__, Runs.__table__.columns.keys())
    print(OptimalFlows.__tablename__, OptimalFlows.__table__.columns.keys())
    print(OptimalNodes.__tablename__, OptimalNodes.__table__.columns.keys())
    print(Solution.__tablename__, Solution.__table__.columns.keys())

if __name__ == "__main__":

    Session = sessionmaker(bind=engine)
    session = Session()

    # raw_row = session.query(RawHANA).first()
    # print(raw_row, '\n')

    

    # raw_baseline = session.query(Baselines).first()
    # print(raw_baseline)

    # lane = session.query(ScenarioLanes).first()
    # print(lane)

    ships = session.query(ShipRank).first()
    print(ships, '\n')

    session.commit()

    test_auto_mapping()

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