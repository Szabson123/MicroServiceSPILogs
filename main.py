import threading
import asyncio
from fastapi import FastAPI, Depends
from sqlalchemy import text
from contextlib import asynccontextmanager

from database import engine, get_db
from utils.utils import working_test, fetch_ASM_databases
from utils.scheduler import run_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    # test DB
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Połączone z bazą danych")
    except Exception as e:
        print("Błąd połączenia z DB:", e)

    # URUCHAMIASZ SCHEDULER TYLKO RAZ I W ODDZIELNYM WĄTKU
    threading.Thread(target=run_scheduler, daemon=True).start()

    # FastAPI gotowe
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/hello/")
def hello():
    return {"message": "Hello working"}


@app.get("/working-test/")
def working_test_api(db=Depends(get_db)):
    return working_test(db)


@app.get("/api/new-product-poke/{id}/")
def new_product_poke(id: int):
    return id

@app.get("/api/fetch_ASM_databases/")
def test_asm(db=Depends(get_db)):
    return fetch_ASM_databases(db)