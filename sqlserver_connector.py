# %%
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from os import getenv

# %%


class SqlConnector():
    def __init__(self, dotenvpath: str, dbname: str):
        load_dotenv(dotenvpath)
        self.dbname = dbname
        self.user = getenv('user')
        self.password = getenv('pass')
        self.servername = getenv('servername')


# %%
connector = SqlConnector(".env", "FONTES_DB")
engine = create_engine(
    f"mssql+pyodbc://{connector.user}:{connector.password}@{connector.servername}:1433/{connector.dbname}?driver=ODBC+Driver+17+for+SQL+Server")
