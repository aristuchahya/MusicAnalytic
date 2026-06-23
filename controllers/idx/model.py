from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey, BigInteger, Date
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class TradingInfo(Base):
    __tablename__ = 'idx_company_trading_info'
    __table_args__ = {"schema": "public"}

    id = Column(String, primary_key=True)
    company_code = Column(String)
    company_name = Column(String)
    date = Column(Date)
    open_price = Column(Float)
    high = Column(Float)
    low = Column(Float)
    closing = Column(Float)
    volume = Column(Float)
    value = Column(Float)
    frequency = Column(Float)
    metadata_id = Column(String, ForeignKey('public.metadata_idx_company.id'))

    metadata_detail = relationship(
        "MetadataTable",
        back_populates="trading_info",
    )


class MetadataTable(Base):
    __tablename__ = 'metadata_idx_company'
    __table_args__ = {"schema": "public"}

    id = Column(String, primary_key=True)
    link = Column(String)
    tags = Column(ARRAY(String))
    source = Column(String)
    path_data_raw = Column(String)
    crawling_time = Column(DateTime)

    trading_info = relationship(
        "TradingInfo",
        back_populates="metadata_detail"
    )
