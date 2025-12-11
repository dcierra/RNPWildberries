from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class SupplierStock(Base):
    __tablename__ = 'supplier_stocks'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    date_receiving = Column(Date)
    last_change_date_at = Column(DateTime)
    warehouse_name = Column(String)
    supplier_article = Column(String)
    nm_id = Column(BigInteger)
    barcode = Column(String)
    quantity = Column(Integer)
    in_way_to_client = Column(Integer)
    in_way_from_client = Column(Integer)
    quantity_full = Column(Integer)
    category = Column(String)
    subject = Column(String)
    brand = Column(String)
    tech_size = Column(String)
    price = Column(Float)
    discount = Column(Float)
    is_supply = Column(Boolean)
    is_realization = Column(Boolean)
    sc_code = Column(String)
