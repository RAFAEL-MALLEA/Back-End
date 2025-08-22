from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database.main_db import SessionLocal
from database.dynamic import get_tenant_session
from models.main_db import Company
from models.tenant_db import Product
from schemas.product import ProductCreate, ProductOut

router = APIRouter()

def get_main_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/products", response_model=list[ProductOut])
def get_products(company_id: int, db: Session = Depends(get_main_db)):
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)
    products = tenant_session.query(Product).all()
    return products

@router.post("/products", response_model=ProductOut)
def create_product(data: ProductCreate, company_id: int, db: Session = Depends(get_main_db)):
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)
    new_product = Product(**data.dict())
    tenant_session.add(new_product)
    tenant_session.commit()
    tenant_session.refresh(new_product)
    return new_product