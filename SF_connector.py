# %%
import snowflake.connector
from dotenv import load_dotenv
from os import getenv

# %%


class SFConnector():
    def __init__(self):
        load_dotenv(".env")
        self.snowflakeuser = getenv('snowflakeuser')
        self.snowflakepassword = getenv('snowflakepassword')
        self.snowflakeaccname = getenv('snowflakeaccname')

    def conn(self):
        conn = snowflake.connector.connect(
            user=self.snowflakeuser,
            password=self.snowflakepassword,
            account=self.snowflakeaccname
        )
        return conn
