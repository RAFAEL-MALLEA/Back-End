from sqlalchemy.orm import Session, joinedload
from models.main_db import Company
from models.main_db import User

def get_company_by_id(db: Session, company_id: int):
    return db.query(Company).options(
        joinedload(Company.integration)
    ).filter(Company.id == company_id).first()

def get_company_by_rut(db: Session, rut: str):
    return db.query(Company).filter(Company.rut == rut).first()

def get_all_companies(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Company).offset(skip).limit(limit).all()

def get_companies_for_user(db: Session, user_id: int):
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        return user.companies
    return []

def get_specific_company_for_user(db: Session, user_id: int, company_id: int):
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        for company in user.companies:
            if company.id == company_id:
                return company
    return None