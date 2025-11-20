from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import settings


DATABASE_URL = (
    f"postgresql://{settings.user}:"
    f"{settings.password}@"
    f"{settings.host}:"
    f"{settings.port}/"
    f"{settings.db_name}"
)

engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
