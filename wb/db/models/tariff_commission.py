from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class TariffCommission(Base):
    __tablename__ = 'tariffs_commission'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    upload_at = Column(Date)
    kgvp_marketplace = Column(Float)
    kgvp_supplier = Column(Float)
    kgvp_supplier_express = Column(Float)
    paid_storage_kgvp = Column(Float)
    parent_id = Column(Integer)
    parent_name = Column(String)
    subject_id = Column(Integer)
    subject_name = Column(String)
