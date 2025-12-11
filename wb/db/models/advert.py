from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class Advert(Base):
    __tablename__ = 'advert_list'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    advert_type = Column(Integer)
    advert_status = Column(Integer)
    advert_count = Column(Integer)

    advert_id = Column(Integer)
    change_time_at = Column(DateTime)
