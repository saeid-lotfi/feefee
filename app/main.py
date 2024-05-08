from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import threading

from database import get_db, init_db
from sync import schedule_update_db
# from models import *
from api_functions import get_overview, get_chart_data, get_assets

import logging
logging.basicConfig(
    filename='/logs/db_sync.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')

# initialize the server database
init_db()

# Start the background task in a separate thread
background_thread = threading.Thread(target= schedule_update_db)
background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
background_thread.start()

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
