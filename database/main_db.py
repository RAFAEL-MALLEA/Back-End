from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

MAIN_DATABASE_URL = "postgresql://inventaria:NhQsFpmSjD3LwQc@inventaria-db-prod.cbaqc3csfgja.us-east-1.rds.amazonaws.com:5432/main"

engine = create_engine(MAIN_DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()