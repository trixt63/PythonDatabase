from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker


class Base(DeclarativeBase):
    pass


class TableStats(Base):
    def __init__(self, table_name):
        # super().__init__()
        __tablename__ = table_name

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


class Dog:
    def __init(self, name):
        __name__ = name

    n_legs = 4


if __name__ == '__main__':
    new_dog = Dog('Maika')
    print(new_dog.n_legs)