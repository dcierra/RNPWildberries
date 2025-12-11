from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class AdvertNMReportExtended(Base):
    __tablename__ = 'advert_nm_report_extended'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    nm_id = Column(BigInteger)
    dt_on = Column(Date)
    open_card_count = Column(Integer)
    add_to_cart_count = Column(Integer)
    orders_count = Column(Integer)
    orders_sum_rub = Column(Integer)
    buyouts_count = Column(Integer)
    buyouts_sum_rub = Column(Integer)
    cancel_count = Column(Integer)
    cancel_sum_rub = Column(Integer)
    buyout_percent = Column(Integer)
    add_to_cart_conversion = Column(Float)
    cart_to_order_conversion = Column(Float)
