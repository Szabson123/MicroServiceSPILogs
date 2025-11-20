import logging
from sqlalchemy.orm import Session
from .database import SessionLocal
from .utils import working_test

logger = logging.getLogger("apscheduler")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("scheduler.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logger.addHandler(handler)


def scheduled_task():
    """Funkcja wywoływana cyklicznie przez APScheduler."""
    logger.info("Start zadania scheduler'a")

    try:
        db: Session = SessionLocal()
        result = working_test(db)
        logger.info(f"Wynik zadania: {result}")

    except Exception as e:
        logger.error(f"Błąd w zadaniu: {e}")

    finally:
        db.close()

    logger.info("Koniec zadania\n")
