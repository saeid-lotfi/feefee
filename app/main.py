from typing import List
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from database import get_db
from models import IndexHistory


app = FastAPI()

@app.get("/")
def posts():
    return {"message": "Welcome to FeeFee"}

@app.get("/index_history")
def get(db: Session = Depends(get_db)):
    data = db.query(IndexHistory).all()
    return data
