# %%
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from os import getenv

# %%
load_dotenv(
    "C:\\Users\\marcus\\OneDrive - Climate-KIC\\IAAC\\Plaza_Mirror_Ingestion\\.env")

user = getenv('user'),
password = getenv('pass')
servername = getenv('servername')
print(user)

# %%
engine = create_engine(
    f"mssql+pyodbc://{user}:{password}@{servername}:1433/FONTES_DB?driver=ODBC+Driver+17+for+SQL+Server")

# %%
sql = '''
SELECT top 10 *
FROM dbo."tab_fato001" tf 
'''

# %%
# querying:
df = pd.read_sql_query(sql, engine)

# %%
df

# %%
df.to_csv("plaza_mirror.csv", sep=";")

# %%
# running stuff in the SQL server:
sql = '''
CREATE TABLE testing (id int, name varchar(50))
'''

# %%
engine.execute(sql)

# %%
engine.close()

# %%
# engine.dispose()
