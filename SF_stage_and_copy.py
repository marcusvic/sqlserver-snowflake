import pandas as pd
from get_sqlserver_metadata import create_table
from SF_connector import SFConnector

# %%
conn = SFConnector().conn()

# %%
conn.cursor().execute("USE WAREHOUSE DEMO_WH")
# %%
conn.cursor().execute("USE DATABASE TEST")
# %%
conn.cursor().execute("USE SCHEMA TEST.PUBLIC")
# %%
# testing
#sql = "CREATE OR REPLACE TABLE test_table(DATA_FATO datetime, COD_DIA varchar, COD_CLIENTE varchar, COD_FABRICA varchar, COD_PRODUTO varchar, COD_ORGANIZACIONAL varchar, FATURAMENTO float, IMPOSTO float, CUSTO_VARIAVEL float, UNIDADE_VENDIDA float, QUANTIDADE_VENDIDA float)"
conn.cursor().execute(create_table)

# %%

# %%
# conn.cursor().execute("put file://testing.csv @%test_table \
#      auto_compress=true overwrite=true;")
# %%
# conn.cursor().execute("COPY INTO test_table from @%test_table/testing.csv.gz \
#     file_format = (format_name = CSV_FORMAT)")

# %%
#conn.cursor().execute("SELECT * FROM test.\"PUBLIC\".tab_fato001")

# %%
conn.cursor().execute("put file://TAB_FATO001.csv @%tab_fato001 \
    auto_compress=true overwrite=true;")
# %%
conn.cursor().execute("COPY INTO TAB_FATO001 \
                FROM @%tab_fato001/TAB_FATO001.csv.gz \
                FILE_FORMAT=(format_name = CSV_FORMAT) \
                on_error=continue")
