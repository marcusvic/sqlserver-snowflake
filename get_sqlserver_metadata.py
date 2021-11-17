# %%
from sqlalchemy.sql.expression import table
from sqlserver_connector import SqlConnector
import pandas as pd

# %%
# given an MSS Table and Schema, writes the respective CREATE TABLE statement in SnowFlake


class CreateTableStatement(SqlConnector):
    # SQL SERVER - Snowflake data types mapping
    __sql_snowflake_map = dict(
        int="number",
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
        varbinary="binary",
        uniqueidentifier="varchar"
    )

    def __init__(self, dbname: str, schema: str, table_name: str):
        super().__init__(dbname)
        self.schema = schema
        self.table_name = table_name

    # gets metadata from a SQL server table, given a schema and a table name
    def gets_table_metada(self):
        return f'''
        select column_name, data_type, character_maximum_length 
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='{self.table_name}' and table_schema = '{self.schema}'
        '''

    # creates a Pandas dataframe with SQL Server column name, data_type, length and Snowflake corresponding datatype
    def creates_df(self):
        df = pd.read_sql_query(
            self.gets_table_metada(),
            SqlConnector(self.dbname).connector()
        )
        df['sf_data_type'] = df['data_type'].map(self.__sql_snowflake_map)
        return df

    # writes "create table" statement to be used in SF with "columns" str containing column names and datatypes
    def writes_create_table_with_columns_and_types(self, df):
        columns = ''
        for n in range(df.shape[0]):
            columns += df.iloc[n, ][0] + ' ' + df.iloc[n, ][3] + ', '
            # columns = ' '.join([str(v) for v in list(df.column_name)])
        return f"CREATE OR REPLACE TABLE {self.table_name}({columns[:-2]})"


# %%
createtable = CreateTableStatement("DW_SUCOS", "dbo", "Dim_Organizacional")
df = createtable.creates_df()
sql_statement = createtable.writes_create_table_with_columns_and_types(df)
print(sql_statement)

# %%
schema = "dbo"
sql = f'''
select table_name 
from information_schema.tables t 
where table_schema = '{schema}'
order by table_name '''

result = list(SqlConnector("DW_SUCOS").connector().execute(sql))
names = [r[0] for r in result]
names

# %%
for each in names:
    createtable = CreateTableStatement("DW_SUCOS", "dbo", each)
    df = createtable.creates_df()
    sql_statement = createtable.writes_create_table_with_columns_and_types(df)
    print(sql_statement)
