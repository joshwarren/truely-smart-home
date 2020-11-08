"""Database connection manager

This class is intended to manage the connection to a given database.
It's allow a single API for both SQL Server and PostgreSQL connections.
It is specifically set up to allow the class to be used within a
contex manager. This ensures that connections are automatically closed
and not accidentally left open.
"""

from typing import List, Optional, Union, Dict, Tuple

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

    def create_scd_history(self, table: str, schema: Optional[str] = None):
        self.create_schema('history')

        history_table = table if schema is None else f'{schema}_{table}'
        schema = schema_check(schema)

        # UNION removes creation of identity
        self.connection.execute(f"""
            SELECT *
            INTO history.{history_table}
            FROM {schema}.{table}
            LIMIT 0
            UNION
            SELECT *
            FROM {schema}.{table}
            LIMIT 0
            """)

        # ****************** Not finished *****************

    def table_exists(self, tableName: str, schema: Optional[str] = None) -> bool:
        """
        NB: This method will also return False if schema does not exist.
        """
        schema = self.schema_check(schema)

        result = self.connection.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
                AND table_name = '{tableName}'
            """)
        return result.rowcount > 0

    def create_fields(self, fields: Union[List[str], str], tableName: str,
                      schema: Optional[str] = None,
                      dtypes: Optional[Union[List, Dict, str]] = None):
        """
        NB: default datatype is VARCHAR(MAX)
        """
        schema = self.schema_check(schema)

        if type(fields) is str:
            fields = [fields]

        if dtypes is not None:
            if isinstance(dtypes, str):
                # Single field
                dtypes = [dtypes]
            if isinstance(dtypes, dict):
                dtypes = [self._get_SQL_datatypes(
                    dtypes.get(f, sqlalchemy.types.String())) for f in fields]
            else:
                dtypes = [self._get_SQL_datatypes(dt) for dt in dtypes]
        else:
            dtypes = ['VARCHAR(MAX)'] * len(fields)

        assert len(dtypes) == len(
            fields), f"One dtype ({len(dtypes)} supplied) must be supplied for each field ({len(fields)} supplied)"

        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{tableName}'
                AND table_schema = '{schema}'
            """
        result = self.connection.execute(sql)
        assert result.rowcount > 0, f"Table {schema}.{tableName} does not exist"

        columns = [f['column_name'].lower() for f in result]

        for field, dtype in zip(fields, dtypes):
            if field.lower() not in columns:
                sql = f"""
                    ALTER TABLE {schema}.{tableName}
                    ADD COLUMN {field.lower()} {dtype}
                    """
                self.session.execute(sql)
                self.session.commit()

    def set_field_names_to_lower_case(self, df: Union[pd.DataFrame, dict]
                                      ) -> Union[pd.DataFrame, dict]:
        if isinstance(df, pd.DataFrame):
            df.columns = map(str.lower, df.columns)
            return df
        elif isinstance(df, dict):
            return {k.lower(): v for k, v in df.items()}

    def dataframe_to_table(self, df: pd.DataFrame, tableName: str,
                           schema: Optional[str] = None,
                           dtype: Optional[Dict] = None,
                           dedup: bool = False):
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

        from logger import create_logger
        logger = create_logger('db')
        logger.info(f'Writing to {schema}.{tableName}')

        df = self.set_field_names_to_lower_case(df)
        dtype = self.set_field_names_to_lower_case(dtype)

        # Check all required fields exist
        if self.table_exists(tableName, schema):
            if dtype is None:
                dtype = dict(self._get_column_names_and_types(df))

            self.create_fields(df.columns, tableName, schema, dtype)

        # write data
        df.to_sql(tableName, self.connection, schema=schema, index=False,
                  if_exists='append', dtype=dtype, method='multi',
                  chunksize=1000)
        self.session.commit()
        if dedup:
            self.dedup(tableName, schema)

    def has_changed(self, new: Union[Dict, pd.Series, pd.DataFrame],
                    tableName: str, schema: str, orderBy: str,
                    reverse: bool = False) -> bool:
        new = self.set_field_names_to_lower_case(new)
        orderBy = orderBy.lower()

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

    @staticmethod
    def _sqlalchemy_type(col):
        """Cast pandas datatype to sqlalchemy
        Code adapted from pandas-dev:
        https://github.com/pandas-dev/pandas/blob/a0c8425a5f2b74e8a716defd799c4a3716f66eff/pandas/io/sql.py#L1019
        """

        # Infer type of column, while ignoring missing values.
        # Needed for inserting typed data containing NULLs, GH 8778.
        import pandas._libs.lib as lib
        col_type = lib.infer_dtype(col, skipna=True)

        from sqlalchemy.types import (
            TIMESTAMP,
            BigInteger,
            Boolean,
            Date,
            DateTime,
            Float,
            Integer,
            Text,
            Time,
        )

        if col_type == "datetime64" or col_type == "datetime":
            # GH 9086: TIMESTAMP is the suggested type if the column contains
            # timezone information
            try:
                if col.dt.tz is not None:
                    return TIMESTAMP(timezone=True)
            except AttributeError:
                # The column is actually a DatetimeIndex
                # GH 26761 or an Index with date-like data e.g. 9999-01-01
                if getattr(col, "tz", None) is not None:
                    return TIMESTAMP(timezone=True)
            return DateTime
        if col_type == "timedelta64":
            warnings.warn(
                "the 'timedelta' type is not supported, and will be "
                "written as integer values (ns frequency) to the database.",
                UserWarning,
                stacklevel=8,
            )
            return BigInteger
        elif col_type == "floating":
            if col.dtype == "float32":
                return Float(precision=23)
            else:
                return Float(precision=53)
        elif col_type == "integer":
            if col.dtype == "int32":
                return Integer
            else:
                return BigInteger
        elif col_type == "boolean":
            return Boolean
        elif col_type == "date":
            return Date
        elif col_type == "time":
            return Time
        elif col_type == "complex":
            raise ValueError("Complex datatypes not supported")

        return Text

    def _get_column_names_and_types(self, df: pd.DataFrame) -> List[Tuple[str, sqlalchemy.types.TypeEngine]]:
        """ Taken and adapted from:
        https://github.com/pandas-dev/pandas/blob/a0c8425a5f2b74e8a716defd799c4a3716f66eff/pandas/io/sql.py#L926
        """
        column_names_and_types = []
        if df.index is not None:
            for i, idx_label in enumerate(df.index.names):
                idx_type = self._sqlalchemy_type(df.index.get_level_values(i))
                column_names_and_types.append((str(idx_label), idx_type))

        column_names_and_types += [
            (str(df.columns[i]), self._sqlalchemy_type(df.iloc[:, i]))
            for i in range(len(df.columns))
        ]

        return column_names_and_types

    def _get_SQL_datatypes(self, sqlalchemy_dtype: Union[sqlalchemy.types.TypeEngine, str]) -> str:
        if isinstance(sqlalchemy_dtype, sqlalchemy.types.TypeEngine):
            return sqlalchemy_dtype.compile(self.engine.dialect)
        else:
            return sqlalchemy_dtype

    def dedup(self, table: str, schema: str):
        sql = f"""
        CREATE TEMPORARY TABLE dedupped AS
        SELECT *
        FROM {schema}.{table} t
        UNION
        SELECT *
        FROM {schema}.{table} t2;

        TRUNCATE TABLE {schema}.{table};

        INSERT INTO {schema}.{table}
        SELECT *
        FROM dedupped;

        DROP TABLE dedupped;
        """

        self.session.execute(sql)
        self.session.commit()
