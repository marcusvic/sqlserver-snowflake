# %%
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from os import getenv

# %%
load_dotenv(
    "C:\\Users\\marcus\\OneDrive - Climate-KIC\\IAAC\\Plaza_Mirror_Ingestion\\.env")

user = getenv('user')
password = getenv('pass')
servername = getenv('servername')

# %%
engine = create_engine(
    f"mssql+pyodbc://{user}:{password}@{servername}:1433/FONTES_DB?driver=ODBC+Driver+17+for+SQL+Server")


# %%
sql = '''
select name
from sys.tables t 
'''

result = list(engine.execute(sql))
names = [r[0] for r in result]

# %%
for name in names:
    sql = f'''
    SELECT *
    FROM dbo."{name}" tf 
    '''
    df = pd.read_sql_query(sql, engine)
    df.to_csv(f"{name}.csv", sep=",", index=False, header=True)


# %%
# running stuff in the SQL server:
# sql = '''
# INSERT INTO "testing"
# values
#     (4, "Mar,cus,"),
#     (5, "Rabea, a"),
# '''

# engine.execute(sql)

# %%
# sql = f'''
#     SELECT *
#     FROM dbo."testing"
#     '''
# df = pd.read_sql_query(sql, engine)
# df.to_csv(f"testing.csv", sep=";", index=False)

# %%
# engine.close()

# %%
engine.dispose()
