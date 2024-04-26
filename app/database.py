from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import logging
logging.getLogger().setLevel(logging.INFO)

from models import Base

# SQLALCHEMY_DATABASE_URL = "postgresql://username:password@localhost:5432/feefee"
SQLALCHEMY_DATABASE_URL = "postgresql://username:password@db/feefee"

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit= False, autoflush= False, bind= engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():

    metadata = MetaData()
    metadata.reflect(bind= engine)

    if Base.metadata.tables.keys() == metadata.tables.keys():
        logging.info('Tables are already created!')
    
    else:
        logging.info('Tables not found in database!')
        Base.metadata.drop_all(engine)
        logging.info('Creating Tables ...')
        Base.metadata.create_all(engine)
