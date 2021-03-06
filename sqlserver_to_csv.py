# %%
import pandas as pd
from sqlserver_connector import SqlConnector

# %%

engine = SqlConnector("FONTES_DB").connector()

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
    FROM dbo."{name}"
    '''
    df = pd.read_sql_query(sql, engine)
    df.to_csv(f"{name}.csv", sep=",", index=False, header=True)

# %%
engine.dispose()
