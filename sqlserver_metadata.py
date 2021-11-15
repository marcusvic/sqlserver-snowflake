# %%
from sqlserver_connector import engine
import pandas as pd

# %%
sql = f'''
    select column_name, data_type, character_maximum_length 
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_NAME='TAB_FATO001' and table_schema = 'dbo'
    '''
df = pd.read_sql_query(sql, engine)
df.to_csv(f"metadata.csv", sep=",", index=False, header=True)
