import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy import Table, Column, Float, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.automap import automap_base 

from env import DB_CONN_PARAMETER

## use stg engine, but prefix WI tables with WI


engine = create_engine(DB_CONN_PARAMETER)
metadata = MetaData()
auto_Base = automap_base(metadata=metadata)

metadata.reflect(engine)

class ShipRank(auto_Base):
    __tablename__ = 'ShipRank'

    def __repr__(self):
        return "ShipRank(ship_type = {}, ship_rank = {})".format(self.ship_rank, self.ship_type)

class Lanes(auto_Base):
    __tablename__ = 'Lanes'

    def __repr__(self):
        return "{}: ({}_{}_{}_{}) -> ({}_{}_{}_{}) | <ship_type: {}, pflow: {}, path: {}, rank: {}, alpha: {}, (d, f): ({}, {})> | in_pflow: {}".format(
            self.pdct_fam,
            self.ori_name, self.ori_country, self.ori_region, self.ori_role,
            self.desti_name, self.desti_country, self.desti_region, self.desti_role,
            self.ship_type, self.pflow, self.path, self.path_rank, self.alpha,
            self.d, self.f,
            self.in_pflow
        )
        # return "{}: ({} - {} - {} - {}) -> ({} - {}) | ({}, {}) -> ({} - {} - {} - {})".format(
        #     self.pdct_fam, 
        #     self.ori_name, self.ori_country, self.ori_region, self.ori_role,
        #     self.ship_type, self.ship_rank,
        #     self.d, self.f,
        #     self.desti_name, self.desti_country, self.desti_region, self.desti_role
        # )

    def get_successors(self, session):
        stmt = text("""
        select * from "Lanes" 
        where d > {} and f < {}
        """).format(self.d, self.f)
        return session.execute(stmt)


class pdct_fam_types(auto_Base):
    __tablename__ = 'pdct_fam_types'

    def __repr__(self):
        return "{}: {}".format(self.pdct_fam, self.pdct_type)


class Nodes(auto_Base):

    __tablename__ = 'Nodes'

    def __repr__(self):
        return '{}_{}_{}_{}: {}'.format(self.name, self.country, self.region, self.role, self.pdct_fam)


class Edges(auto_Base):

    __tablename__ = 'Edges'

    def __repr__(self):
        return None

auto_Base.prepare()
RawLanes = auto_Base.classes.raw_lanes

