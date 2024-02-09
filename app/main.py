from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from database import get_db
from utils import init_db
from models import IndexHistory

init_db()

app = FastAPI()

@app.get("/")
def posts():
    return {"message": "Welcome to FeeFee"}

@app.get("/index_history/{type}")
def get(db: Session = Depends(get_db)):
    data = db.query(IndexHistory).filter(IndexHistory.Index_Type == 'boors').all()
    return data

