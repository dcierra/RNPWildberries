from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class FbsWarehouse(Base):
    __tablename__ = 'stat_fbs_warehouses'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    name = Column(String)
    office_id = Column(Integer)
    warehouse_id = Column(Integer)
    cargo_type = Column(Integer)
    delivery_type = Column(Integer)
