from decimal import FloatOperation
from networkx.algorithms.shortest_paths.unweighted import single_source_shortest_path_length
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

from datetime import datetime

from env import DB_CONN_PARAMETER_CLOUD, DB_CONN_PARAMETER_STG, DB_CONN_PARAMETER_WI, DB_CONN_PARAMETER, DB_CONN_PARAMETER_PROD, DB_CONN_POST

## use stg engine, but prefix WI tables with WI


# engine_local = create_engine(DB_CONN_PARAMETER)

# stg_engine = create_engine(DB_CONN_PARAMETER_STG)
# stg_metadata = MetaData()
# stg_metadata.reflect(stg_engine, extend_existing=True) # extend_existing
# stg_Base = automap_base(metadata=stg_metadata)
# print('\n', metadata.tables.keys(), '\n')




engine = create_engine(DB_CONN_PARAMETER_CLOUD)
metadata = MetaData()
metadata.reflect(engine) # extend_existing
Base = automap_base(metadata=metadata)
# print('\n', metadata.tables.keys(), '\n')

class Baselines(Base):
    
    baseline_id = Column('baseline_id', String, primary_key=True, nullable=True)
    date = Column('date', Date)
    description = Column('description', String)
    start = Column('start', String)
    end = Column('end', String)

    __tablename__ = 'scdsi_baselines'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return 'Baseline {}:  {} to {} - {} - {}'.format(
            self.baseline_id, self.start, self.end, self.date, self.description
            )


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


class DecomNodes(Base):

    decom_node_id = Column('decom_node_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    region = Column('region', String)

    __tablename__ = 'scdsi_decommisioned_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}, {}) | {} - <{}_{}_{}_{}>'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, self.name, self.region, self.role
            )

    @classmethod
    def get_decom_nodes(cls, baseline_id, scenario_id, session):
        decom_nodes = session.query(cls).filter(
            cls.baseline_id == baseline_id,
            cls.scenario_id == scenario_id).all()
        return [((n.name, n.region, n.role), n.pdct_fam) for n in decom_nodes]


class AltNodes(Base):

    alt_node_id = Column('alt_node_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    region = Column('region', String)
    alt_name = Column('alt_name', String)
    alt_region = Column('alt_region', String)
    supply = Column('supply', Float)
    capacity = Column('capacity', Float)
    opex = Column('opex', Float)

    __tablename__ = 'scdsi_alternative_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | {} - <{}_{}_{}_{}> replaces <({}_{}_{}_{})> | supply: {}, capacity: {}, opex'.format(
            self.baseline_id, self.scenario_id, self.pdct_fam, 
            self.alt_name, self.alt_region, self.role,
            self.name, self.region, self.role,
            self.supply, self.capacity, self.opex
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
            self.ori_name, self.ori_region, self.ori_role,
            self.desti_name, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f
        ) 


class DecomEdges(Base):

    decom_edge_id = Column('decom_edge_id', Integer, primary_key=True, nullable=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
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
            self.ori_name, self.ori_region, self.ori_role,
            self.desti_name, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f
        ) 


class ScenarioLanes(Base):

    scenario_row_id = Column('scenario_row_id', Integer, primary_key=True, autoincrement=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
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
        return "({}, {}) | {}: ({}_{}_{}) -> ({}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})> | {}".format(
            self.scenario_id, self.baseline_id,
            self.pdct_fam,
            self.ori_name, self.ori_region, self.ori_role,
            self.desti_name, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f,
            self.in_pflow
        )

    def get_successors(self, session):
        successors = session.query(ScenarioLanes).filter(
            ScenarioLanes.scenario_id == self.scenario_id,
            ScenarioLanes.baseline_id == self.baseline_id,
            ScenarioLanes.in_pflow == 1,
            ScenarioLanes.pdct_fam == self.pdct_fam,
            ScenarioLanes.path_rank == self.path_rank + 1,
            ScenarioLanes.d > self.d,
            ScenarioLanes.f < self.f
        ).order_by(ScenarioLanes.d).all()
        return successors

    def get_lanes(scenario_id, baseline_id, node_to_index, mode_to_index, session, pdct_to_index=None):
        stmt = text(
            """
            select * 
            from scdsi_scenario_lanes l
            left join scdsi_scenario_edges e
            on l.baseline_id = e.baseline_id
            and l.scenario_id = e.scenario_id
            and l.ori_name = e.ori_name
            and l.ori_region = e.ori_region
            and l.desti_name = e.desti_name
            and l.desti_region = e.desti_region
            where l.in_pflow = 1  
            and l.baseline_id = :baseline_id
            and l.scenario_id = :scenario_id
            """
        ).params(
            scenario_id = scenario_id,
            baseline_id = baseline_id 
        )
        lanes = session.execute(stmt).all()

        return {
            (
                node_to_index[(x.pdct_fam, x.ori_name, x.ori_region, x.ori_role)], 
                node_to_index[(x.pdct_fam, x.desti_name, x.desti_region, x.desti_role)], 
                # pdct_to_index[x.pdct_fam], 
                mode_to_index[x.transport_mode]
            ): {
                    'transport_cost': x.transport_cost,
                    'transport_time': x.transport_time,
                    'co2e': x.co2e
                } 
                for x in lanes
            }

    @classmethod
    def get_specified_lanes(cls, scenario_id, baseline_id, node_to_index, session):
        lanes = session.query(cls).filter(
            cls.scenario_id == scenario_id,
            cls.baseline_id == baseline_id,
            cls.in_pflow == 1
        ).all()

        ret = {
            'Customer': [],
            'Gateway': [],
            'DSLC': [],
            'OSLC': [],
            'DF': [],
            'PCBA': [],
            'GHUB': [],
        }

        for lane in lanes:
            index = node_to_index[(lane.pdct_fam, lane.desti_name, lane.desti_region, lane.desti_role)]
            ret[lane.desti_role] += [(lane.pdct_fam, lane.desti_name, lane.desti_region, lane.desti_role)]

        return ret

    @classmethod
    def get_pdct_maps(cls, scenario_id, baseline_id, session):
        pdct_fams = session.query(ScenarioLanes.pdct_fam).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.in_pflow == 1
            ).distinct().all()
        return {pdct_fams[i][0]: i for i in range(len(pdct_fams))}, {i: pdct_fams[i][0] for i in range(len(pdct_fams))}

    @classmethod
    def get_manufacturing_adjacency_list(cls, scenario_id, baseline_id, node_to_index, session, pdct_to_index=None):
        manuf = session.query(cls).filter(
            ScenarioLanes.scenario_id == scenario_id,
            ScenarioLanes.baseline_id == baseline_id,
            ScenarioLanes.in_pflow == 1,
            ScenarioLanes.parent_pflow == None,
            ScenarioLanes.ori_role.in_(['DF', 'PCBA'])
        ).all()

        ret = {}

        # start = datetime.now()
        for m in manuf:
            index = node_to_index[(m.pdct_fam, m.ori_name, m.ori_region, m.ori_role)]
            # product = pdct_to_index[m.pdct_fam]
            ret[index] = {}
            adj = m.get_successors(session)
            d = 0
            for i in adj:
                if i.d != d:
                    ret[index][i.d] = [(node_to_index[(i.pdct_fam, i.ori_name, i.ori_region, i.ori_role)], i.alpha)]
                else:
                    ret[index][i.d] += [(node_to_index[(i.pdct_fam, i.ori_name, i.ori_region, i.ori_role)], i.alpha)]
        # print(datetime.now() - start)
        return ret

class Locations(Base):

    location_id = Column(Integer, primary_key=True, autoincrement=True, nullable=True)
    name = Column('name', String)
    region = Column('region', String)
    lat = Column('lat', Float)
    long = Column('long', Float)

    __tablename__ = 'scdsi_locations'
    __table_args__ = {'extend_existing': True}

    def __repr__(self):
        return '({}_{}_{}) | Latitude: {} - Longitude: {}'.format(
            self.name, self.region, self.lat, self.long
        )

    @classmethod
    def get_locations(cls, session):
        locations = session.query(cls).all()
        return {(x.name, x.region): {'lat': x.lat, 'long': x.long} for x in locations}

class ScenarioEdges(Base):

    scenario_edge_id = Column(Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    ori_name = Column('ori_name', String)
    ori_region = Column('ori_region', String)
    desti_name = Column('desti_name', String)
    desti_region = Column('desti_region', String)
    transport_mode = Column('transport_mode', String)
    transport_time = Column('transport_time', Float)
    co2e = Column('co2e', Float)
    transport_cost = Column('transport_cost', Float)
    distance = Column('distance', Float)
    total_weight = Column('total_weight', Float)
    in_pflow = Column('in_pflow', Integer)

    __tablename__ = 'scdsi_scenario_edges'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}) --> ({}_{}_{}): | {} | distance = {}, time = {}, co2 = {}'.format(
            self.scenario_id, self.baseline_id,
            self.ori_name, self.ori_region,
            self.desti_name, self.desti_region,
            self.transport_mode,
            self.distance, self.transport_time, self.co2e
        )

    @classmethod
    def get_scenario_edges(cls, scenario_id, baseline_id, session):
        scenario_edges = session.query(cls).filter(
            cls.baseline_id == baseline_id,
            cls.scenario_id == scenario_id
        ).all()
        return {(
            x.ori_name, 
            x.ori_region, 
            x.desti_name, 
            x.desti_region,
            x.transport_mode): {
                'cost': x.transport_cost, 
                'time': x.transport_time, 
                'co2e': x.co2e} for x in scenario_edges}

class ScenarioNodes(Base):

    scenario_node_id = Column(Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    region = Column('region', String)
    supply = Column('supply', Float)
    capacity = Column('capacity', Float)
    opex = Column('opex', Float)
    pflow = Column('pflow', Integer)
    in_pflow = Column('in_pflow', Integer)

    __tablename__ = 'scdsi_scenario_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}) | ({}_{}_{}_{}) | supply: {}, capacity: {}, opex: {}'.format(
            self.scenario_id, self.baseline_id,
            self.name, self.region, self.role,
            self.supply, self.capacity, self.opex
        )

    @classmethod
    def get_node_maps(cls, scenario_id, baseline_id, session):
        nodes = session.query(cls).filter(
            ScenarioNodes.scenario_id == scenario_id,
            ScenarioNodes.baseline_id == baseline_id,
            ScenarioNodes.in_pflow == 1
        )
        return {x.scenario_node_id: {
            'name': (x.pdct_fam, x.name, x.region, x.role),
            'supply': x.supply,
            'capacity': x.capacity,
            'opex': x.opex
        } for x in nodes}, {(x.pdct_fam, x.name, x.region, x.role): x.scenario_node_id for x in nodes}



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

    opt_flow_id = Column('opt_flow_id', Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    run_id = Column('run_id', Integer, ForeignKey('scdsi_runs.run_id'))
    pdct_fam = Column('pdct_fam', String)
    ori_name = Column('ori_name', String)
    ori_region = Column('ori_region', String)
    ori_role = Column('ori_role', String, default='')
    desti_name = Column('desti_name', String)
    desti_region = Column('desti_region', String)
    desti_role = Column('desti_role', String, default='')
    transport_mode = Column('transport_mode', String)

    __tablename__ = 'scdsi_optimal_flows'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return "({}, {}, {}) | {}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <transport_mode: {}, flow: {}>".format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.pdct_fam,
            self.ori_name, self.ori_region, self.ori_role,
            self.desti_name, self.desti_region, self.desti_role,
            self.transport_mode, self.flow
        )


class OptimalNodes(Base):

    opt_node_id = Column('opt_node_id', Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    run_id = Column('run_id', Integer, ForeignKey('scdsi_runs.run_id'))
    pdct_fam = Column('pdct_fam', String)
    role = Column('role', String)
    name = Column('name', String)
    region = Column('region', String)
    state = Column('state', Integer)

    __tablename__ = 'scdsi_optimal_nodes'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | ({}_{}_{}_{}) | state: {}'.format(
            self.run_id, self.scenario_id, self.baseline_id, 
            self.name, self.region, self.role,
            self.state
        )


class Solution(Base):

    solution_id = Column(Integer, primary_key=True)
    baseline_id = Column('baseline_id', String, ForeignKey('scdsi_baselines.baseline_id'))
    scenario_id = Column('scenario_id', Integer, ForeignKey('scdsi_scenarios.scenario_id'))
    run_id = Column('run_id', Integer, ForeignKey('scdsi_runs.run_id'))
    optimal_cost = Column('optimal_cost', Float)
    optimal_co2e = Column('optimal_co2e', Float)
    optimal_time = Column('optimal_time', Float)

    __tablename__ = 'scdsi_solution'
    __table_args__ = {'extend_existing': True}

    def __repr__(self) -> str:
        return '({}, {}, {}) | <Optimal Cost: {}, Optimal Time: {}, Optimal CO2e: {}>'.format(
            self.run_id, self.scenario_id, self.baseline_id,
            self.optimal_cost, self.optimal_time, self.optimal_co2e
        )


Base.prepare()

def test_auto_mapping():
    print(Baselines.__tablename__, Baselines.__table__.columns.keys())
    print(Scenarios.__tablename__, Scenarios.__table__.columns.keys())
    print(DecomNodes.__tablename__, DecomNodes.__table__.columns.keys())
    print(AltNodes.__tablename__, AltNodes.__table__.columns.keys())
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

    start = datetime.now()

    Session = sessionmaker(bind=engine)
    session = Session()

    # raw_row = session.query(RawHANA).first()
    # print(raw_row, '\n')

    

    # raw_baseline = session.query(Baselines).first()
    # print(raw_baseline)

    lane = session.query(Locations).first()
    print(lane)

    # location = Locations(name="atlanta", country="US", region="US", lat=10, long=1)
    # session.add(location)

    # ships = session.query(ScenarioLanes).first()
    # print(ships, '\n')
    print(datetime.now() - start)

    session.commit()

    # test_auto_mapping()

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