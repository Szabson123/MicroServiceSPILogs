import asyncio
import logging
from sqlalchemy.orm import Session
from database import SessionLocal
from .utils import working_test

logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("scheduler.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logger.addHandler(handler)


async def scheduler_worker():
    """Uruchamiane w tle co 15 sekund"""
    logger.info("Scheduler wystartowal")

    while True:
        try:
            db: Session = SessionLocal()
            result = working_test(db)
            logger.info(f"Wynik: {result}")

        except Exception as e:
            logger.error(f"Błąd w schedulerze: {e}")

        finally:
            db.close()

        await asyncio.sleep(15)


def run_scheduler():
    """Uruchamia async scheduler w osobnym wątku"""
    asyncio.run(scheduler_worker())
