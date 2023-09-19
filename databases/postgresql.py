from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from typing import List

from config import PostgresDBConfig
from utils.logger_utils import get_logger

logger = get_logger('PostgreSQL')


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()
        pass

    def get_all_schemas(self):
        schemas = self.session.execute("""
            SELECT schema_name
            FROM information_schema.schemata;
        """)
        self.session.commit()
        return [schema[0] for schema in schemas]

    def get_all_table(self, schema):
        tables = self.session.execute(f"""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = '{schema}';
                    """)
        return [table['table_name'] for table in tables]

    ###################################
    #      Wallet Address Table       #
    ###################################

    def query_on_all_chains(self, schemas: List = None):
        if not schemas:
            schemas = [schema for schema in self.get_all_schemas() if schema[:5] == 'chain']
        for schema in schemas:
            query = f"""
                select {schema}.smart_contract
            """
            self.session.execute(query)
            self.session.commit()
            print(f"Finish query on {schema}")

    def grant_select(self, schemas: List, tables: List, username):
        for schema in schemas:
            query = f"""
                GRANT USAGE ON SCHEMA {schema} TO {username};
            """
            for table in tables:
                query += f"""
                    GRANT SELECT ON {schema}.{table} TO {username};
                """
            self.session.execute(query)
            self.session.commit()
            logger.info(f"Grant select on {schema}: {tables} for {username}")

    def grant_usage_schema(self, schema: str, username: str):
        q = f"GRANT USAGE ON SCHEMA {schema} TO {username}"
        self.session.execute(q)
        self.session.commit()
        return 0


if __name__ == '__main__':
    pg = PostgresDB()
    schemas = ['chain_0x38', 'chain_0x1', 'chain_0xfa', 'chain_0x89']
    username = 'community_detection'
    pg.grant_select(schemas=schemas, tables=['token_transfer'], username=username)
