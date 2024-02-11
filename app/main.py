from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from database import get_db
from utils import init_db
from models import *

init_db()

app = FastAPI()

@app.get("/")
def posts():
    return {"message": "Welcome to FeeFee"}

@app.get("/index_history/{index_type}")
def get(index_type: str, db: Session = Depends(get_db)):
    data = db.query(IndexHistory).filter(IndexHistory.Index_Type == index_type).all()
    return data

@app.get("/fund_history/{fund_id}")
def get(fund_id: int, db: Session = Depends(get_db)):
    data = db.query(FundHistory).filter(FundHistory.Fund_Id == fund_id).all()
    return data