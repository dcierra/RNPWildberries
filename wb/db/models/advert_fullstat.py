from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class AdvertFullStat(Base):
    __tablename__ = 'advert_full_stats'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    date_at = Column(DateTime)
    app_type = Column(Integer)
    name = Column(String)
    nm_id = Column(BigInteger)
    views = Column(Integer)
    clicks = Column(Integer)
    ctr = Column(Float)
    cpc = Column(Float)
    sum = Column(Float)
    atbs = Column(Integer)
    orders = Column(Integer)
    cr = Column(Float)
    shks = Column(Integer)
    sum_price = Column(Float)
    avg_position = Column(Integer)
    advert_id = Column(BigInteger)
    canceled = Column(Integer)
