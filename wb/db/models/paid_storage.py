from sqlalchemy import Column, BigInteger, Date, DateTime, String, Integer, Float, Boolean

from wb.db.connector import Base


class PaidStorage(Base):
    __tablename__ = 'paid_storage'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    date_on = Column(Date)
    log_warehouse_coef = Column(Float)
    office_id = Column(Integer)
    warehouse = Column(String)
    warehouse_coef = Column(Float)
    gi_id = Column(BigInteger)
    chrt_id = Column(BigInteger)
    size = Column(String)
    barcode = Column(String)
    subject = Column(String)
    brand = Column(String)
    vendor_code = Column(String)
    nm_id = Column(BigInteger)
    volume = Column(Float)
    calc_type = Column(String)
    warehouse_price = Column(Float)
    barcodes_count = Column(Integer)
    pallet_place_code = Column(Integer)
    pallet_count = Column(Float)
    original_date_on = Column(Date)
