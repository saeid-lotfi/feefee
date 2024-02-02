from sqlalchemy import String, Boolean, Integer, Column, Float, BigInteger, Date
from database import Base


class IndexHistory(Base):
    __tablename__ = 'IndexHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Index_Id = Column(BigInteger, nullable= False)
    Index_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)
    
    