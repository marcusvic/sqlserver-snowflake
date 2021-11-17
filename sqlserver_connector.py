# %%
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from os import getenv

# %%


class SqlConnector():
    def __init__(self, dbname: str):
        load_dotenv(".env")
        self.dbname = dbname
        self.user = getenv('user')
        self.password = getenv('pass')
        self.servername = getenv('servername')

    def connector(self):
        connector = create_engine(
            f"mssql+pyodbc://{self.user}:{self.password}@{self.servername}:1433/{self.dbname}?driver=ODBC+Driver+17+for+SQL+Server")
        return connector
