from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class SupplierOrder(Base):
    __tablename__ = 'supplier_orders'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    date_on = Column(DateTime)
    last_change_date_on = Column(DateTime)
    warehouse_name = Column(String)
    warehouse_type = Column(String)
    country_name = Column(String)
    oblast_okrug_name = Column(String)
    region_name = Column(String)
    supplier_article = Column(String)
    nm_id = Column(BigInteger)
    barcode = Column(String)
    category = Column(String)
    subject = Column(String)
    brand = Column(String)
    tech_size = Column(String)
    income_id = Column(BigInteger)
    is_supply = Column(Boolean)
    is_realization = Column(Boolean)
    total_price = Column(Float)
    discount_percent = Column(Integer)
    spp = Column(Float)
    finished_price = Column(Float)
    price_with_disc = Column(Float)
    is_cancel = Column(Boolean)
    cancel_date_at = Column(DateTime)
    order_type = Column(String)
    sticker = Column(String)
    g_number = Column(String)
    srid = Column(String)
