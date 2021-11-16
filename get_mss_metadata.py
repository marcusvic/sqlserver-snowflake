# %%
from sqlserver_connector import engine
import pandas as pd

# %%
sql = f'''
    select column_name, data_type, character_maximum_length 
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_NAME='tab_fato001' and table_schema = 'dbo'
    '''
df = pd.read_sql_query(sql, engine)

# %%
sql_snowflake_map = dict(
    smallint="number",
    smallmoney="number",
    tinyint="number",
    float="float",
    real="float",
    date="date",
    datetime2="timestamp_ntz",
    datetime="datetime",
    datetimeoffset="timestamp_ltz",
    smalldatetime="datetime",
    timestamp="timestamp_ntz",
    time="time",
    char="varchar(1)",
    text="varchar",
    varchar="varchar",
    nchar="varchar",
    ntext="varchar",
    nvarchar="varchar",
    binary="binary",
    varbinary="binary"
)

# %%
df['sf_data_type'] = df['data_type'].map(sql_snowflake_map)

# %%
columns = ''
for n in range(df.shape[0]):
    columns += df.iloc[n, ][0] + ' ' + df.iloc[n, ][3] + ', '

columns = columns[:-2]

# columns = ' '.join([str(v) for v in list(df.column_name)])
# %%
# print(columns)

create_table = f"CREATE OR REPLACE TABLE tab_fato001({columns})"

# %%
# print(sql)
# %%
#df.to_csv(f"metadata.csv", sep=",", index=False, header=True)
