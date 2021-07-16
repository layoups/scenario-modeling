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

engine = create_engine(URL(
                user='SCDS_SCDSI_ETL_SVC',
                password='&p5dr#Hm8g',
                account='cisco.us-east-1',
                warehouse='SCDS_SCDSI_ETL_WH',
                database='SCDS_DB',
                schema='SCDS_SCDSI_STG',
                role = 'SCDS_SCDSI_ETL_ROLE'
                ))

metadata = MetaData()
metadata.reflect(engine, extend_existing=True) # extend_existing

print(metadata.tables.keys())


auto_Base = automap_base(metadata=metadata)

class Baselines(auto_Base):

    __tablename__ = 'scdsi_baselines'

    def __repr__(self):
        return '{}: {} - {}'.format(self.baseline_id, self.date, self.description)

class ShipRank(auto_Base):

    __tablename__ = 'scdsi_ship_rank'

    def __repr__(self):
        return "ShipRank(ship_type = {}, ship_rank = {})".format(self.ship_rank, self.ship_type)


class Scenarios(auto_Base):

    __tablename__ = 'scdsi_scenarios'


class Omega(auto_Base):

    baseline_id = Column(Integer, primary_key=True)

    __tablename__ = 'scds_scdsi_wi.scdsi_omega'

# class Lanes(auto_Base):
#     __tablename__ = 'Lanes'

#     def __repr__(self):
#         return "{}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})>".format(
#             self.pdct_fam,
#             self.ori_name, self.ori_country, self.ori_region, self.ori_role,
#             self.desti_name, self.desti_country, self.desti_region, self.desti_role,
#             self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
#             self.d, self.f
#         )
#         # return "{}: ({} - {} - {} - {}) -> ({} - {}) | ({}, {}) -> ({} - {} - {} - {})".format(
#         #     self.pdct_fam, 
#         #     self.ori_name, self.ori_country, self.ori_region, self.ori_role,
#         #     self.ship_type, self.ship_rank,
#         #     self.d, self.f,
#         #     self.desti_name, self.desti_country, self.desti_region, self.desti_role
#         # )

#     def get_successors(self, session):
#         stmt = text("""
#         select * from "Lanes" 
#         where d > {} and f < {}
#         """).format(self.d, self.f)
#         return session.execute(stmt)


# class pdct_fam_types(auto_Base):
#     __tablename__ = 'pdct_fam_types'

#     def __repr__(self):
#         return "{}: {}".format(self.pdct_fam, self.pdct_type)


# class Nodes(auto_Base):

#     __tablename__ = 'Nodes'

#     def __repr__(self):
#         return '{}_{}_{}_{}: {}'.format(self.name, self.country, self.region, self.role, self.pdct_fam)


# class Edges(auto_Base):

#     __tablename__ = 'Edges'

#     def __repr__(self):
#         return None


# class Locations(auto_Base):

#     __tablename__ = 'Locations'

#     def __repr__(self):
#         return '{}_{}_{}: <latitude: {}>, <longitude: {}>'.format(
#             self.name, 
#             self.country, 
#             self.region,
#             self.lat,
#             self.long
#         )

auto_Base.prepare()
# RawLanes = auto_Base.classes.raw_lanes

if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()

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


    ship_types = session.query(Baselines).first()
    print(ship_types)