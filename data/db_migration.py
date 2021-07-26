import numpy as np
import pandas as pd

from sqlalchemy import create_engine, desc, or_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from env import *

engine = create_engine(DB_CONN_PARAMETER_PROD)
data = pd.read_excel('data/XFM_COST_20.xlsx')
# data = data.fillna(2)

data.to_sql('"SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_RAW_XFM_OPEX"', con=engine, if_exists='replace', index=False)


