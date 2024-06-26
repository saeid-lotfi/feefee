from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session


from database import get_db, init_db
from sync import schedule_sync_db
from api_functions import get_overview, get_chart_data, get_assets

# set logger
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("fastapi")

# initialize the server database
init_db()

# start the sync process
schedule_sync_db()

# app instance
app = FastAPI()

@app.get("/")
def posts():
    return {"message": "Welcome to FeeFee"}

@app.get("/get-overview")
def get(db: Session = Depends(get_db)):
    data = get_overview(db)
    return data

@app.get("/get-chart-data")
def get(db: Session = Depends(get_db), candle: str = '1M', from_time: str = '2024-01-01', to_time: str = '2024-02-01'):
    data = get_chart_data(db, candle, from_time, to_time)
    return data

@app.get("/get-assets")
def get(db: Session = Depends(get_db)):
    data = get_assets(db)
    return data
