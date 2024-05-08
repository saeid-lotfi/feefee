from sqlalchemy import String, Boolean, Column, Float, BigInteger, Date, PrimaryKeyConstraint
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

    Crypto_Id = Column(BigInteger, nullable= False)
    Date_En = Column(Date, nullable= False)
    Crypto_Type = Column(String, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Crypto_Id', 'Date_En'),
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
    Price_Closing = Column(Float, nullable= False)
    Price_Yesterday = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

    __table_args__ = (
        PrimaryKeyConstraint('Fund_Id', 'Date_En'),
    )
    is_view = False



######### views

class TotalIndex_view(Base):
    stmt = select(
        BourseHistory.Index_Type,
        BourseHistory.Date_En,
        BourseHistory.Value,
        ((BourseHistory.Value - func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)).label('daily_rate_of_change'),
        ((BourseHistory.Value - func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)).label('weekly_rate_of_change'),
        ((BourseHistory.Value - func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)).label('monthly_rate_of_change'),
        ).where(BourseHistory.Index_Type == "total").order_by(BourseHistory.Date_En.desc()).limit(1)
    is_view = True
    __table__ = create_view(
        name= 'TotalIndex_view', 
        selectable= stmt, 
        metadata= Base.metadata,
        replace= True
        )

class WeightedIndex_view(Base):
    stmt = select(
        BourseHistory.Index_Type,
        BourseHistory.Date_En,
        BourseHistory.Value,
        ((BourseHistory.Value - func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)).label('daily_rate_of_change'),
        ((BourseHistory.Value - func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)).label('weekly_rate_of_change'),
        ((BourseHistory.Value - func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)).label('monthly_rate_of_change'),
        ).where(BourseHistory.Index_Type == "weighted").order_by(BourseHistory.Date_En.desc()).limit(1)
    is_view = True
    __table__ = create_view(
        name= 'WeightedIndex_view', 
        selectable= stmt, 
        metadata= Base.metadata,
        replace= True
        )

class Cryptocurrency_view(Base):
    stmt = select(
        CryptocurrencyHistory.Crypto_Type,
        CryptocurrencyHistory.Date_En,
        CryptocurrencyHistory.Value,
        ((CryptocurrencyHistory.Value - func.lag(CryptocurrencyHistory.Value).over(order_by= CryptocurrencyHistory.Date_En)) / func.lag(CryptocurrencyHistory.Value).over(order_by= CryptocurrencyHistory.Date_En)).label('daily_rate_of_change'),
        ((CryptocurrencyHistory.Value - func.lag(CryptocurrencyHistory.Value, 7).over(order_by= CryptocurrencyHistory.Date_En)) / func.lag(CryptocurrencyHistory.Value, 7).over(order_by= CryptocurrencyHistory.Date_En)).label('weekly_rate_of_change'),
        ((CryptocurrencyHistory.Value - func.lag(CryptocurrencyHistory.Value, 30).over(order_by= CryptocurrencyHistory.Date_En)) / func.lag(CryptocurrencyHistory.Value, 30).over(order_by= CryptocurrencyHistory.Date_En)).label('monthly_rate_of_change'),
        ).where(CryptocurrencyHistory.Crypto_Type == "bitcoin").order_by(CryptocurrencyHistory.Date_En.desc()).limit(1)
    is_view = True
    __table__ = create_view(
        name= 'Cryptocurrency_view', 
        selectable= stmt, 
        metadata= Base.metadata,
        replace= True
        )

