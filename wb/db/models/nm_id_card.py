from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class NmIDCard(Base):
    __tablename__ = 'nmids_list'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    nm_id = Column(BigInteger)
    imt_id = Column(Integer)
    nm_uuid = Column(String)
    subject_id = Column(String)
    subject_name = Column(String)
    vendor_code = Column(String)
    brand = Column(String)
    title = Column(String)
    barcode = Column(String)
    chrt_id = Column(Integer)
    tech_size = Column(String)
    wb_size = Column(String)
    length = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
