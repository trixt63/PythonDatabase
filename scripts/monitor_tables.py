from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class QueryResult(Base):
    __tablename__ = 'query_results'

    id = Column(Integer, primary_key=True)
    current_time = Column(DateTime)
    schemaname = Column(String)
    relname = Column(String)
    tuples_inserted = Column(Integer)
    tuples_deleted = Column(Integer)
    live_tuples = Column(Integer)
    dead_tuples = Column(Integer)
    last_vacuum = Column(DateTime)
    last_autovacuum = Column(DateTime)
    tuples_inserted_since_vacuum = Column(Integer)
    table_size = Column(Integer)
    index_size = Column(Integer)

# Replace the database URL with your actual database connection string
DATABASE_URL = "postgresql://username:password@localhost:5432/your_database"
engine = create_engine(DATABASE_URL)

Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()

def update_query_result():
    result = session.execute("""
        SELECT 
            current_timestamp as current_time,
            schemaname, 
            relname, 
            n_tup_ins as tuples_inserted, 
            n_tup_del as tuples_deleted, 
            n_live_tup as live_tuples, 
            n_dead_tup as dead_tuples,
            last_vacuum, 
            last_autovacuum,  
            n_ins_since_vacuum as tuples_inserted_since_vacuum, 
            pg_relation_size(concat(schemaname, '.', relname)) as table_size, 
            pg_indexes_size(concat(schemaname, '.', relname)) as index_size
        FROM 
            pg_stat_user_tables 
        WHERE 
            relname = 'token_transfer'
            AND schemaname = 'chain_0x1'
        ORDER BY 
            n_live_tup DESC
    """).fetchall()

    for row in result:
        query_result = QueryResult(
            current_time=row.current_time,
            schemaname=row.schemaname,
            relname=row.relname,
            tuples_inserted=row.tuples_inserted,
            tuples_deleted=row.tuples_deleted,
            live_tuples=row.live_tuples,
            dead_tuples=row.dead_tuples,
            last_vacuum=row.last_vacuum,
            last_autovacuum=row.last_autovacuum,
            tuples_inserted_since_vacuum=row.tuples_inserted_since_vacuum,
            table_size=row.table_size,
            index_size=row.index_size
        )
        session.add(query_result)

    session.commit()

# Call the update function every 5 minutes (adjust as needed)
# This is just an example; you might use a proper scheduler in production.
while True:
    update_query_result()
    time.sleep(300)  # 5 minutes
