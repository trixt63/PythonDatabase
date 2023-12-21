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
    #            Monitoring           #
    ###################################
    def get_tables_size(self, schema: str = 'public') -> dict:
        query = f"""
            SELECT table_name,
                   pg_size_pretty(pg_total_relation_size(concat('{schema}.', table_name)))
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER by table_name"""
        cursor = self.session.execute(query)
        self.session.commit()
        return {row['table_name']: row['pg_size_pretty'] for row in cursor}

    ###################################
    #      Wallet Address Table       #
    ###################################

    def query_on_all_chains(self, schemas: List = None):
        if not schemas:
            schemas = [schema for schema in self.get_all_schemas() if schema.startWith('chain')]
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
            # self.session.execute(query)
            # self.session.commit()
            print(query)
            # logger.info(f"Grant select on {schema}: {tables} for {username}")

    def revoke_before_delete(self, schemas: List, tables: List, username):
        for schema in schemas:
            query = ""
            for table in tables:
                query += f"""
                    REVOKE ALL PRIVILEGES ON {schema}.{table} FROM {username};
                """
            query += f"""
                REVOKE ALL PRIVILEGES ON SCHEMA {schema} FROM {username};
            """
            self.session.execute(query)
            self.session.commit()
            print(query)
            # logger.info(f"Grant select on {schema}: {tables} for {username}")

    def grant_usage_schema(self, schema: str, username: str):
        q = f"GRANT USAGE ON SCHEMA {schema} TO {username}"
        self.session.execute(q)
        self.session.commit()
        return 0


if __name__ == '__main__':
    pg = PostgresDB()
    schemas = ['chain_0x38', 'chain_0x1', 'chain_0xfa', 'chain_0x89',
               'chain_0xa', 'chain_0xa86a', 'chain_0xa4b1', 'chain_0x2b6653dc']
    username = 'token_transfer_reader_2'
    tables = ['token_transfer', 'token_decimals']
    pg.revoke_before_delete(schemas=schemas, tables=tables, username=username)
