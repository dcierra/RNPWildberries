from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class AdvertNMReport(Base):
    __tablename__ = 'advert_nm_report'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    nm_id = Column(BigInteger)
    imt_name = Column(String)
    vendor_code = Column(String)

    dt_on = Column(Date)
    open_card_count = Column(Integer)
    add_to_cart_count = Column(Integer)
    orders_count = Column(Integer)
    orders_sum_rub = Column(Integer)
    buyouts_count = Column(Integer)
    buyouts_sum_bub = Column(Integer)
    buyout_percent = Column(Integer)
    add_to_cart_conversion = Column(Float)
    cart_to_order_conversion = Column(Integer)
