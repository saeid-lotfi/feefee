from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect, text
import time

from models import Base

# set logger
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
init_db_logger = logging.getLogger("InitDB")
init_db_logger.addHandler(logging.FileHandler("/logs/init_db.log"))
init_db_logger.propagate = False

####################
# SQLALCHEMY_DATABASE_URL = "postgresql://username:password@localhost:5432/feefee"
SQLALCHEMY_DATABASE_URL = "postgresql://username:password@db/feefee"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit= False, autoflush= False, bind= engine)

####################
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():

    while True:
        try:
            init_db_logger.info("Trying to connect to the database...")
            engine.connect()
            init_db_logger.info("Successfully connected to the database!")
            break
        except OperationalError as e:
            init_db_logger.info(f"Failed to connect to the database: {e}")
            init_db_logger.info("Retrying in 5 seconds...")
            time.sleep(5)

    # Get database inspector
    inspector = inspect(engine)
    
    # Get existing tables and views
    existing_tables = inspector.get_table_names()
    init_db_logger.info(f'DB tables {existing_tables}')
    existing_views = inspector.get_view_names()
    init_db_logger.info(f'DB views {existing_views}')

    # existing_tables = db_metadata.tables
    # existing_views = db_metadata.views
    model_tables = [cls.__table__ for cls in Base.__subclasses__() if not getattr(cls, 'is_view', False)]
    init_db_logger.info(f'Model tables {[table.name for table in model_tables]}')
    model_views = [cls.__table__ for cls in Base.__subclasses__() if getattr(cls, 'is_view', False)]
    init_db_logger.info(f'Model views {[view.name for view in model_views]}')

    
    # tables
    for table in model_tables:
        table_name = table.name 
        init_db_logger.info(f'Checking table {table_name}')
        # if table exist in db
        if table_name in existing_tables:
            existing_columns = [column['name'] for column in inspector.get_columns(table_name)]
            model_columns = table.c.keys()
            # if schema match
            if set(existing_columns) == set(model_columns):
                init_db_logger.info(f'Table {table_name} exists and schema match!')
            # if schema not match
            else:
                init_db_logger.info(f'Table {table_name} exists but schema does not match between model and database!')
                init_db_logger.info(f'Database columns for {table_name}: {existing_columns}')
                init_db_logger.info(f'Model columns for {table_name}: {model_columns}')
                init_db_logger.info(f'Dropping table {table_name}...')
                table.drop(engine)
                init_db_logger.info(f'Recreating table {table_name}...')
                table.create(engine)
        # if table not exist in db
        else:
            init_db_logger.info(f'Table {table_name} does not exist in the database! Creating table...')
            # Base.metadata.create_all(engine, [table])
            table.create(engine)

    # views
    init_db_logger.info(f'Views will be dropped ...')
    
    for view_name in [view.name for view in model_views]:
        init_db_logger.info(f'Dropping view {view_name} ...')
        db_gen = get_db()
        db = next(db_gen)
        try:
            stmt = text(f'DROP VIEW IF EXISTS "public"."{view_name}" CASCADE')
            db.execute(stmt)
            db.commit()
        except Exception as e:
            # Rollback the transaction in case of error
            db.rollback()
            raise e    
        finally:
            db.close()
        
    init_db_logger.info(f'Views will be created again ...')
    Base.metadata.create_all(engine)

    init_db_logger.info(f'Init DB finished successfully!')
