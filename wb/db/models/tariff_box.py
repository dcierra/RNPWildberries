from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class TariffBox(Base):
    __tablename__ = 'tariffs_box'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    upload_at = Column(Date)
    warehouse_name = Column(String)
    box_delivery_and_storage_expr = Column(Float)
    box_delivery_base = Column(Float)
    box_delivery_liter = Column(Float)
    box_storage_base = Column(Float)
    box_storage_liter = Column(Float)
