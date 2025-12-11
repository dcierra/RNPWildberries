from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class AdvertCost(Base):
    __tablename__ = 'advert_costs'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    upd_num = Column(Integer)
    upd_time_at = Column(DateTime)
    upd_sum = Column(Integer)
    advert_id = Column(Integer)
    camp_name = Column(String)
    advert_type = Column(Integer)
    payment_type = Column(String)
    advert_status = Column(Integer)
