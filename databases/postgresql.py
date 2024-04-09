from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from typing import List

from config import PostgresDBConfig
from utils.logger_utils import get_logger

logger = get_logger('PostgreSQL')

Session = sessionmaker()


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)
        Session.configure(bind=self.engine)

    @staticmethod
    def get_all_schemas():
        with Session.begin() as session:
            schemas = session.execute("""
                SELECT schema_name
                FROM information_schema.schemata;
            """)
        return [schema[0] for schema in schemas]

    @staticmethod
    def get_all_table(schema):
        with Session.begin() as session:
            tables = session.execute(f"""
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = '{schema}';
                      """)
        return [table['table_name'] for table in tables]

    ###################################
    #            Monitoring           #
    ###################################
    @staticmethod
    def get_tables_size(schema: str = 'public') -> dict:
        with Session.begin() as session:
            query = f"""
                SELECT table_name,
                       pg_size_pretty(pg_total_relation_size(concat('{schema}.', table_name))), 
                       pg_total_relation_size(concat('{schema}.', table_name))
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER by table_name"""
            cursor = session.execute(query)
        return {row['table_name']: row['pg_size_pretty'] for row in cursor}

    @staticmethod
    def get_tables_sizes_on_all_schema(table_name: str) -> dict:
        with Session.begin() as session:
            query = f"""
                SELECT table_schema, table_name, 
                	   pg_size_pretty(pg_total_relation_size(concat(table_schema, '.', table_name))), 
                	   pg_total_relation_size(concat(table_schema, '.', table_name))
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
                ORDER by table_name
            """
            cursor = session.execute(query)
        # return {row['table_schema']: row['pg_size_pretty'] for row in cursor}
        return {row['table_schema']: row['pg_total_relation_size'] for row in cursor}

    @staticmethod
    def get_tables_n_rows(table_name: str) -> dict:
        with Session.begin() as session:
            query = f"""SELECT 
                        schemaname
                        ,relname
                        ,n_live_tup AS EstimatedCount 
                    FROM pg_stat_user_tables 
                    where relname = '{table_name}'
                    ORDER BY n_live_tup DESC;"""
            data = session.execute(query).all()
        return {row['schemaname']: [row['estimatedcount']] for row in data}

    @staticmethod
    def get_detailed_table_data_on_all_schemas(table_name: str) -> dict:
        with Session.begin() as session:
            query = f"""SELECT t2.table_name, t2.table_schema, 
                    t2.total_size as total_size, t2.data_size as data_size, t2.index_size as index_size, 
                    t1.n_live_tup as n_live_tup, t1.n_tup_ins as n_tup_ins, t1.n_tup_del as n_tup_del
                FROM 
                    (SELECT schemaname, relname, 
                     n_tup_ins, n_tup_del, n_live_tup
                     FROM pg_stat_user_tables 
                     where relname = '{table_name}'
                     ORDER BY n_live_tup DESC) as t1
                JOIN (SELECT 
                        table_name,
                        table_schema,
                        pg_total_relation_size(concat(table_schema, '.{table_name}')) AS total_size,
                        pg_relation_size(concat(table_schema, '.{table_name}')) AS data_size,
                        pg_indexes_size(concat(table_schema, '.{table_name}')) AS index_size
                      FROM information_schema.tables
                      WHERE table_name = '{table_name}'
                      ORDER by table_name
                ) as t2
                on t1.schemaname = t2.table_schema"""

            data = session.execute(text(query)).all()
        return {row['schemaname']: [row['estimatedcount']] for row in data}

    ###################################
    #      Wallet Address Table       #
    ###################################
    @staticmethod
    def grant_select(schemas: List, tables: List, username):
        for schema in schemas:
            with Session.begin() as session:
                query = f"""
                    GRANT USAGE ON SCHEMA {schema} TO {username};
                """
                for table in tables:
                    query += f"""
                        GRANT SELECT ON {schema}.{table} TO {username};
                    """
                # session.execute(query)
                logger.info(f"Grant select on {schema}: {tables} for {username}")
                # print(query)

    @staticmethod
    def revoke_before_delete(schemas: List, tables: List, username):
        for schema in schemas:
            with Session.begin() as session:
                query = ""
                for table in tables:
                    query += f"""
                        REVOKE ALL PRIVILEGES ON {schema}.{table} FROM {username};
                    """
                query += f"""
                    REVOKE ALL PRIVILEGES ON SCHEMA {schema} FROM {username};
                """
                session.execute(query)
                print(query)
                # logger.info(f"Grant select on {schema}: {tables} for {username}")

    @staticmethod
    def grant_usage_schema(schema: str, username: str):
        with Session.begin() as session:
            q = f"GRANT USAGE ON SCHEMA {schema} TO {username}"
            session.execute(q)
        return 0


if __name__ == '__main__':
    pg = PostgresDB()
    schemas_ = ['chain_0x38', 'chain_0x1', 'chain_0xfa', 'chain_0x89',
                'chain_0xa', 'chain_0xa86a', 'chain_0xa4b1', 'chain_0x2b6653dc']
    username_ = 'token_transfer_reader_2'
    data = pg.get_detailed_table_data_on_all_schemas(table_name='token_transfer')
