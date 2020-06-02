""" Method to set up system """

from db import db
from config import dbConfig

schemas = ['WEATHER', 'SUPPLY', 'CONFIG']

with db(**dbConfig) as DB:

    # Create schemas
    for schema in schemas:
        DB.connection.execute(f"CREATE SCHEMA {schema}")
