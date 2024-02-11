from sqlalchemy import String, Boolean, Integer, Column, Float, BigInteger, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class IndexHistory(Base):
    __tablename__ = 'IndexHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Index_Id = Column(BigInteger, nullable= False)
    Index_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)


class FundHistory(Base):
    __tablename__ = 'FundHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Fund_Id = Column(BigInteger, nullable= False)
    Fund_Name = Column(String, nullable= False)
    Fund_NameDetail = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Price_Closing = Column(Float, nullable= False)
    Price_Yesterday = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)
    
    