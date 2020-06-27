"""Database connection manager

This class is intended to manage the connection to a given database.
It's allow a single API for both SQL Server and PostgreSQL connections.
It is specifically set up to allow the class to be used within a
contex manager. This ensures that connections are automatically closed
and not accidentally left open.
"""


import pyodbc
import sqlalchemy


class db:
    def __init__(self, server: str, database: str = None, username: str = None, password: str = None, port: int = None, dbType='tsql', engine='pyodbc'):
        self.server = server
        self.database = database
        self.port = port
        self.username = username
        self.password = password
        self.dbType = dbType
        self.engineType = engine

        self.create_engine()

    @property
    def create_engine(self):
        if self.engineType == 'pyodbc':
            return self._create_pyodbc_engine
        elif self.engineType == 'SQLAlchemy':
            return self._create_sqlalchemy_engine
        else:
            raise ValueError(
                f"The parameter enigne must be 'pyodbc' or 'SQLAlchemy' not {self.engineType}")

    def _create_pyodbc_engine(self):
        # the database type must be either SQL Server or PostgreSQL
        if self.dbType == 'tsql':
            driverStr = '{SQL Server}'
        elif self.dbType == 'PostgreSQL':
            driverStr = '{PostgreSQL ANSI}'
        else:
            raise ValueError(
                f"The parameter dbType must be 'tsql' or 'PostgreSQL' not {self.dbType}")

        if self.username is None:
            loginStr = 'Trusted_Connection=yes;'
        else:
            loginStr = f'UID={self.username};PWD={self.password};'

        # Create the connection
        self.connectionStr = f"DRIVER={driverStr};SERVER={self.server};" \
            + f"{loginStr}"
        if self.port is not None:
            self.connectionStr += f";PORT={self.port}"

        if self.database is not None:
            self.connectionStr += f"DATABASE={self.database};"

        self.connection = pyodbc.connect(self.connectionStr)

        # The cursor is used to execute SQL on the server
        self.cursor = self.connection.cursor()

    def _create_sqlalchemy_engine(self):
        """For using an SQLAlchemy engine instead of a pyodbc connection"""
        engine_stmt = "postgresql://"

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

    # @property
    # def sqlalchemyEngine(self):
    #     """For using an SQLAlchemy engine instead of a pyodbc connection"""
    #     engine_stmt = "postgresql://"

    #     if self.username is not None:
    #         engine_stmt += self.username
    #         if self.password is not None:
    #             engine_stmt += f":{self.password}"

    #         engine_stmt += '@'

    #     engine_stmt += self.server
    #     if self.port is not None:
    #         engine_stmt += f':{self.port}'
    #     engine_stmt += f'/{self.database}'

    #     # "?trusted_connection=yes"
    #     # engine_stmt = f"postgresql://{self.server}:{self.port}/{self.database}"
    #     return sqlalchemy.create_engine(engine_stmt)

    def create_schema(self, schemaName: str):
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS {schemaName}')
        self.connection.commit()
