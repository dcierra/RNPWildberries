from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class FbsStock(Base):
    __tablename__ = 'stat_stocks_fbs'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    date_on = Column(Date)

    warehouse_id = Column(Integer)
    amount = Column(Integer)
    sku = Column(String)
