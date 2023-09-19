from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, PrimaryKeyConstraint, UniqueConstraint, Index, MetaData
from config import PostgresDBConfig


metadata_obj = MetaData(schema=PostgresDBConfig.SCHEMA)
Base = declarative_base(metadata=metadata_obj)


class TransferEvent(Base):
    __tablename__ = PostgresDBConfig.TRANSFER_EVENT_TABLE
    block_number = Column(Integer)
    log_index = Column(Integer)
    contract_address = Column(String)
    transaction_hash = Column(String)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Numeric)
    __table_args__ = (PrimaryKeyConstraint('block_number', 'log_index'),)

    def to_dict(self):
        return {
            'block_number': self.block_number,
            'log_index': self.log_index,
            'contract_address': self.contract_address,
            'transaction_hash': self.transaction_hash,
            'from_address': self.from_address,
            'to_address': self.to_address,
            'value': self.value
        }
