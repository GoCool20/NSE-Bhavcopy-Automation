from sqlalchemy import Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# -----------------------
# Equity / Dividend Records
# -----------------------

class BcRecord(Base):
    """
    Model for 'Bc' files (e.g., Bc040625.csv)
    """
    __tablename__ = 'bc_records'
    id = Column(Integer, primary_key=True)
    series = Column(String(10))
    symbol = Column(String(50))
    security = Column(String(100))
    record_dt = Column(String(20))
    bc_strt_dt = Column(String(20), nullable=True)
    bc_end_dt = Column(String(20), nullable=True)
    ex_dt = Column(String(20), nullable=True)
    nd_strt_dt = Column(String(20), nullable=True)
    nd_end_dt = Column(String(20), nullable=True)
    purpose = Column(String(200), nullable=True)


class BhRecord(Base):
    """
    Model for 'bh' files (e.g., bh040625.csv)
    """
    __tablename__ = 'bh_records'
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    series = Column(String(10))
    security = Column(String(100))
    high_low = Column(String(10))
    index_flag = Column(String(10))


class CorpBondRecord(Base):
    """
    Model for 'corpbond' files (e.g., corpbond040625.csv)
    """
    __tablename__ = 'corpbond_records'
    id = Column(Integer, primary_key=True)
    market = Column(String(10))
    series = Column(String(10))
    symbol = Column(String(50))
    security = Column(String(150))
    prev_cl_pr = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    net_trdval = Column(Float, nullable=True)
    net_trdqty = Column(Integer, nullable=True)
    corp_ind = Column(String(10), nullable=True)
    trades = Column(Integer, nullable=True)
    hi_52_wk = Column(Float, nullable=True)
    lo_52_wk = Column(Float, nullable=True)


class EtfRecord(Base):
    """
    Model for 'etf' files (e.g., etf040625.csv)
    """
    __tablename__ = 'etf_records'
    id = Column(Integer, primary_key=True)
    market = Column(String(10))
    series = Column(String(10))
    symbol = Column(String(50))
    security = Column(String(150))
    prev_close_price = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    net_traded_value = Column(Float, nullable=True)
    net_traded_qty = Column(Integer, nullable=True)
    trades = Column(Integer, nullable=True)
    week_52_high = Column(Float, nullable=True)
    week_52_low = Column(Float, nullable=True)
    underlying = Column(String(50), nullable=True)


class GlRecord(Base):
    """
    Model for 'Gl' files (e.g., Gl040625.csv)
    """
    __tablename__ = 'gl_records'
    id = Column(Integer, primary_key=True)
    gain_or_loss = Column(String(10))    # "G" (gain) or "L" (loss)
    security = Column(String(150), index=True)
    close_price = Column(Float)          # Mapped from "CLOSE_PRIC"
    prev_close_price = Column(Float)     # Mapped from "PREV_CL_PR"
    percent_change = Column(Float)       # Mapped from "PERCENT_CG"


class HlRecord(Base):
    """
    Model for 'HL' files (e.g., HL040625.csv)
    """
    __tablename__ = 'hl_records'
    id = Column(Integer, primary_key=True)
    security = Column(String(150))
    new = Column(Float, nullable=True)
    previous = Column(Float, nullable=True)
    new_status = Column(String(10))


class McapRecord(Base):
    """
    Model for 'MCAP' files (e.g., MCAP040625.csv)
    """
    __tablename__ = 'mcap_records'
    id = Column(Integer, primary_key=True)
    trade_date = Column(String(20))
    symbol = Column(String(50))
    series = Column(String(10))
    security_name = Column(String(150))
    category = Column(String(50))
    last_trade_date = Column(String(20))
    face_value = Column(Float, nullable=True)
    issue_size = Column(Integer, nullable=True)
    close_price = Column(Float, nullable=True)
    market_cap = Column(Float, nullable=True)


class PdRecord(Base):
    """
    Model for 'Pd' files (e.g., Pd040625.csv)
    """
    __tablename__ = 'pd_records'
    id = Column(Integer, primary_key=True)
    mkt = Column(String(10))
    series = Column(String(10))
    symbol = Column(String(50))
    security = Column(String(150))
    prev_cl_pr = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    net_trdval = Column(Float, nullable=True)
    net_trdqty = Column(Integer, nullable=True)
    ind_sec = Column(String(10), nullable=True)
    corp_ind = Column(String(10), nullable=True)
    trades = Column(Integer, nullable=True)
    hi_52_wk = Column(Float, nullable=True)
    lo_52_wk = Column(Float, nullable=True)


class PrRecord(Base):
    """
    Model for 'Pr' files (e.g., Pr040625.csv)
    """
    __tablename__ = 'pr_records'
    id = Column(Integer, primary_key=True)
    mkt = Column(String(10))
    security = Column(String(150))
    prev_cl_pr = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    net_trdval = Column(Float, nullable=True)
    net_trdqty = Column(Integer, nullable=True)
    ind_sec = Column(String(10), nullable=True)
    corp_ind = Column(String(10), nullable=True)
    trades = Column(Integer, nullable=True)
    hi_52_wk = Column(Float, nullable=True)
    lo_52_wk = Column(Float, nullable=True)


class SmeRecord(Base):
    """
    Model for 'sme' files (e.g., sme040625.csv)
    """
    __tablename__ = 'sme_records'
    id = Column(Integer, primary_key=True)
    market = Column(String(10))
    series = Column(String(10))
    symbol = Column(String(50))
    security = Column(String(150))
    prev_cl_pr = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    net_trdval = Column(Float, nullable=True)
    net_trdqty = Column(Integer, nullable=True)
    corp_ind = Column(String(10), nullable=True)
    hi_52_wk = Column(Float, nullable=True)
    lo_52_wk = Column(Float, nullable=True)


class TtRecord(Base):
    """
    Model for 'Tt' files (e.g., Tt040625.csv)
    """
    __tablename__ = 'tt_records'
    id = Column(Integer, primary_key=True)
    security = Column(String(150))
    prev_cl_pr = Column(Float, nullable=True)
    close_pric = Column(Float, nullable=True)
    net_trdqty = Column(Integer, nullable=True)
    net_trdval = Column(Float, nullable=True)
