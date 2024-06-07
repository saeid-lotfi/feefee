from sqlalchemy import String, Boolean, Column, Float, Integer, BigInteger, Date, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_view
from sqlalchemy import select, func

Base = declarative_base()


######### tables
class BourseHistory(Base):
    __tablename__ = 'BourseHistory'

    Index_Id = Column(BigInteger, nullable= False)
    Date_En = Column(Date, nullable= False)
    Index_Type = Column(String, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Index_Id', 'Date_En'),
    )
    is_view = False

class CryptocurrencyHistory(Base):
    __tablename__ = 'CryptocurrencyHistory'

    Crypto_Id = Column(String, nullable= False)
    Datetime_Local = Column(DateTime, nullable= False)
    Datetime_UTC = Column(DateTime, nullable= False)
    Date_Local = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Crypto_Id', 'Datetime_Local'),
    )
    is_view = False

class CurrencyHistory(Base):
    __tablename__ = 'CurrencyHistory'

    Currency_Id = Column(BigInteger, nullable= False)
    Date_En = Column(Date, nullable= False)
    Currency_Type = Column(String, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Currency_Id', 'Date_En'),
    )
    is_view = False

class GoldHistory(Base):
    __tablename__ = 'GoldHistory'

    Gold_Id = Column(BigInteger, nullable= False)
    Date_En = Column(Date, nullable= False)
    Gold_Type = Column(String, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Gold_Id', 'Date_En'),
    )
    is_view = False

class FundHistory(Base):
    __tablename__ = 'FundHistory'

    Fund_Id = Column(BigInteger, nullable= False)
    Date_En = Column(Date, nullable= False)
    Fund_Name = Column(String, nullable= False)
    Fund_NameDetail = Column(String, nullable= False)
    Fund_TypeNumber = Column(Integer, nullable= False)
    Fund_TypeName = Column(String, nullable= False)
    Price_Closing = Column(Float, nullable= False)
    Price_Yesterday = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Fund_Id', 'Date_En'),
    )
    is_view = False

######### gold tables
class IndexState(Base):
    __tablename__ = 'IndexState'

    Index_Type = Column(String, nullable= False)
    Date_Local = Column(Date, nullable= False)
    Latest_Value = Column(Float, nullable= False)
    Daily_Rate_Of_Change = Column(Float, nullable= False)
    Weekly_Rate_Of_Change = Column(Float, nullable= False)
    Monthly_Rate_Of_Change = Column(Float, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Index_Type', 'Date_Local'),
    )
    is_view = False

class CryptocurrencyState(Base):
    __tablename__ = 'CryptocurrencyState'

    Crypto_Id = Column(String, nullable= False)
    Date_Local = Column(Date, nullable= False)
    Latest_Value = Column(Float, nullable= False)
    Daily_Rate_Of_Change = Column(Float, nullable= False)
    Weekly_Rate_Of_Change = Column(Float, nullable= False)
    Monthly_Rate_Of_Change = Column(Float, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Crypto_Id', 'Date_Local'),
    )
    is_view = False

class FundState(Base):
    __tablename__ = 'FundState'

    Fund_Id = Column(BigInteger, nullable= False)
    Date_Local = Column(Date, nullable= False)
    Fund_Name = Column(String, nullable= False)
    Fund_NameDetail = Column(String, nullable= False)
    Fund_TypeNumber = Column(Integer, nullable= False)
    Fund_TypeName = Column(String, nullable= False)
    Latest_Value = Column(Float, nullable= False)
    Daily_Rate_Of_Change = Column(Float, nullable= False)
    Weekly_Rate_Of_Change = Column(Float, nullable= False)
    Monthly_Rate_Of_Change = Column(Float, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Fund_Id', 'Date_Local'),
    )
    is_view = False
