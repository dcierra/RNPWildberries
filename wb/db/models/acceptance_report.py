from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class AcceptanceReport(Base):
    __tablename__ = 'acceptance_reports'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    income_id = Column(BigInteger)
    nm_id = Column(BigInteger)
    shk_create_date_on = Column(Date)
    count = Column(Integer)
    gi_create_date_on = Column(Date)
    subject_name = Column(String)
    total = Column(Float)
