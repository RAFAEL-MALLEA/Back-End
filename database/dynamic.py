from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_tenant_session(tenant_db_url):
    engine = create_engine(tenant_db_url)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return Session()