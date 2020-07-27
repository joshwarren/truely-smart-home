"""Database connection manager

This class is intended to manage the connection to a given database.
It's allow a single API for both SQL Server and PostgreSQL connections.
It is specifically set up to allow the class to be used within a
contex manager. This ensures that connections are automatically closed
and not accidentally left open.
"""

from typing import List, Optional, Union, Dict

import pandas as pd
import sqlalchemy
from sqlalchemy import orm

Session = sqlalchemy.orm.sessionmaker()


class db:
    def __init__(self, server: str, database: str = None, username: str = None, password: str = None, port: int = None, dbType='tsql'):
        self.server = server
        self.database = database
        self.port = port
        self.username = username
        self.password = password
        self.dbType = dbType

        if self.dbType == 'tsql':
            engine_stmt = "mssql+pyodbc://"
        elif self.dbType == 'PostgreSQL':
            engine_stmt = "postgresql://"
        else:
            assert False, f"The parameter dbType must be 'tsql' or 'PostgreSQL' not {self.dbType}"

        if self.username is not None:
            engine_stmt += self.username
            if self.password is not None:
                engine_stmt += f":{self.password}"

            engine_stmt += '@'

        engine_stmt += self.server
        if self.port is not None:
            engine_stmt += f':{self.port}'
        engine_stmt += f'/{self.database}'

        # if self.username is None:
        #     engine_stmt += "?trusted_connection=yes"

        self.engine = sqlalchemy.create_engine(engine_stmt)
        self.connection = self.engine.connect()

        self.session = Session(bind=self.connection)

    @property
    def defaultSchema(self):
        if self.dbType == 'tsql':
            return 'dbo'
        elif self.dbType == 'PostgreSQL':
            return 'public'

    def schema_check(self, schema: Union[str, None]) -> str:
        if schema is None:
            schema = self.defaultSchema
        else:
            self.create_schema(schema)
        return schema

    # A connection should be closed when it is finished with as it can start to hog memory

    def close(self):
        self.connection.close()
        if hasattr(self, 'engine'):
            self.engine.dispose()

    # __enter__ and __exit__ methods allow use to use db as a context manager
    # (i.e. it can be called with a with statement so that .close() is
    # automatically called when we're finished)
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # error handling can go in here
        return self.close()

    def create_schema(self, schema: str):
        self.session.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        self.session.commit()

    def table_exists(self, tableName: str, schema: Optional[str] = None) -> bool:
        """
        NB: This method will also return False if schema does not exist.
        """
        schema = self.schema_check(schema)

        result = self.session.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
                AND table_name = '{tableName}'
            """)
        return result.rowcount > 0

    def create_fields(self, fields: Union[List[str], str], tableName: str,
                      schema: Optional[str] = None,
                      dtypes: Optional[Union[List, str]] = None):
        """
        NB: default datatype is VARCHAR(MAX)
        """
        schema = self.schema_check(schema)

        if type(fields) is str:
            fields = [fields]

        if dtypes is not None:
            if type(dtypes) is str:
                dtypes = [dtypes]

            assert len(dtypes) != len(
                fields), "One dtype must be supplied for each field"
        else:
            dtypes = ['VARCHAR(MAX)'] * len(fields)

        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{tableName}'
                AND table_schema = '{schema}''
            """
        result = self.session.execute(sql)
        columns = [f['column_name'] for f in result]

        assert result.rowcount > 0, f"Table {schema}.{tableName} does not exist"

        for field, dtype in zip(fields, dtypes):
            if field not in columns:
                sql = f"""
                    ALTER TABLE {schema}.{tableName}
                    ADD COLUMN {field} {dtype}
                    """
                self.session.excute(sql)
                self.session.commit()

    def dataframe_to_table(self, df: pd.DataFrame, tableName: str,
                           schema: Optional[str] = None,
                           dtype: Optional[Dict] = None):
        """Insert dataframe into SQL table

        This method is a wrapper script for pandas.DataFrame.to_sql which ensures all relevent fields are present in the SQL table. If not, the fields are created and set to NULL for all prior entries.

        Table and schema are created if they do not already exist.

        Args:
            df (pd.DataFrame): pandas DataFrame containing data to be written
            tableName (str): name of table to be written to
            schema (str): name of schema to be written to
        """
        # Create schema if necessary/set default
        schema = self.schema_check(schema)

        # Check all required fields exist
        if self.table_exists(tableName, schema):
            if dtype is None:
                create_dtype = [dt.replace('object', 'TEXT') if type(
                    dt) is str else dt for dt in df.dtypes]
            else:
                create_dtype = []
                for field in df.columns:
                    dt = dtype[field]
                    if dt is sqlalchemy.sql.visitors.VisitableType:
                        create_dtype.append(dt.get_dbapi_type(
                            self.engine.dialect.dbapi))
                    else:
                        create_dtype.append(dt)
            self.create_fields(df.columns, tableName, schema, create_dtype)

        # write data
        df.to_sql(tableName, self.connection, schema=schema, index=False,
                  if_exists='append', dtype=dtype)

    def has_changed(self, new: Union[Dict, pd.Series, pd.DataFrame],
                    tableName: str, schema: str, orderBy: str,
                    reverse: bool = False) -> bool:
        if type(new) is not pd.DataFrame:
            if type(new) is dict:
                new = pd.Series(new)
            new = new.to_frame().T.astype(str)

        if self.table_exists(tableName, schema):
            old = pd.read_sql(f"""
                SELECT *
                FROM {schema}.{tableName}
                ORDER BY "{orderBy}" {"ASC" if reverse else "DESC"}
                LIMIT 1
                """, self.connection).astype(str)
            del old[orderBy]
        else:
            old = pd.DataFrame()

        return not old.equals(new)
