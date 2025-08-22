import traceback
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from .logging_config import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from database.main_db import SessionLocal
from pydantic import BaseModel
from models.main_db import Company
from database.dynamic import get_tenant_session
from utils.bsale import company_bsale, variants_bsale
from utils.bsale.fetch_api import get_bsale
from utils.bsale.process import process_document, sync_bsale_product, sync_bsale_variant,update_stock, update_price
from models.bsale_db import TenantBase

router = APIRouter()

def get_main_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class WebhookData(BaseModel):
    cpnId: int
    resource: str
    resourceId: int
    topic: str
    action: str
    officeId: Optional[int] = None
    priceListId: Optional[int] = None
    send: Optional[int] = None

@router.post("/integrate_bsale")
async def integrate_bsale(
    api_key: str = Query(...),
    company_id: int = Query(...),
    db: Session = Depends(get_main_db)
):
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    
    bsale_company = await company_bsale.get_data(api_key)

    if not bsale_company:
        raise HTTPException(status_code=404, detail="Empresa Bsale no encontrada o API Key inválida")

    print(f"API Key recibida: {api_key}")
    print("Realizando migraciónes...")
    db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    engine = create_engine(db_url)
    TenantBase.metadata.create_all(bind=engine)
    print("Migración completada")
    company.api_key = api_key
    company.external_id = bsale_company['id']

    db.add(company)
    db.commit()
    db.refresh(company)
    return {"message": "Cliente Integrado de forma exitosa!"}

@router.post("/webhook_bsale")
async def webhook_bsale(
    webhook_data: WebhookData,
    background_tasks: BackgroundTasks,
    ):
    logger.info("---------------")
    logger.info("---------------")
    logger.info(f"Procesando webhook: ")
    logger.info(webhook_data)
    logger.info("---------------")
    logger.info("---------------")

    background_tasks.add_task(procesar_webhook, webhook_data)
    return {"message": "Webhook recibido correctamente"}

def procesar_webhook(data: WebhookData):
    db: Session = SessionLocal()
    try:
        cpn_id_str = str(data.cpnId)
        company = db.query(Company).filter_by(external_id=cpn_id_str).first()

        if not company:
            print(f"Error: No se encontró la compañía con external_id {data.cpnId}")
            return
        
        api_key = company.api_key
        print(f"Procesando webhook para compañía ID {company.id}, Tópico: {data.topic}, Recurso API: {data.resource if data.topic in ['document', 'stock'] else data.resourceId}")

        if data.topic == 'document':
            try:
                document_data_list = get_bsale(api_key, endpoint=f"/v1/{data.resource}")
                if document_data_list:
                    document = document_data_list[0] if isinstance(document_data_list, list) else document_data_list
                    print(f"Iniciando process_document para doc ID: {document.get('id')}")
                    process_document(document, api_key, company)
                    print(f"Webhook 'document' (ID: {document.get('id')}) procesado.")
                else:
                    print(f"No se obtuvieron datos del documento para el recurso: {data.resource}")
            except Exception as e:
                print(f"ERROR al procesar webhook 'document' (Recurso API: {data.resource}): {e}")

        elif data.topic == 'stock':
            try:
                print(f"Iniciando update_stock para recurso: {data.resource}")
                update_stock(data.resource, api_key, company)
                print(f"Webhook 'stock' (Recurso API: {data.resource}) procesado.")
            except Exception as e:
                print(f"ERROR al procesar webhook 'stock' (Recurso API: {data.resource}): {e}")
        elif data.topic == 'price':
            try:
                price_list_id = data.priceListId
                variant_id = data.resourceId
                print(f"Iniciando update_price para PriceListID: {price_list_id}, VariantID: {variant_id}")
                update_price(price_list_id, variant_id, api_key, company)
                print(f"Webhook 'price' (PriceListID: {price_list_id}, VariantID: {variant_id}) procesado.")
            except Exception as e:
                print(f"ERROR al procesar webhook 'price' (PriceListID: {data.priceListId}, VariantID: {data.resourceId}): {e}")
        elif data.topic == 'product':
            tenant_session = None
            try:
                tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
                tenant_session = get_tenant_session(tenant_db_url)
                product_id = data.resourceId
                
                print(f"Iniciando sync_bsale_product para ProductID: {product_id}")
                synced_product = sync_bsale_product(tenant_session, api_key, product_id)
                
                if synced_product:
                    tenant_session.commit()
                    print(f"Webhook 'product' (ProductID: {product_id}) procesado y commiteado.")
                else:
                    print(f"sync_bsale_product para ProductID {product_id} no devolvió un objeto. Se hará rollback si hubo cambios en sesión.")
                    tenant_session.rollback()
            except Exception as e:
                print(f"ERROR al procesar webhook 'product' (ProductID: {data.resourceId}): {e}")
                if tenant_session:
                    tenant_session.rollback()
            finally:
                if tenant_session:
                    tenant_session.close()
        elif data.topic == 'variant':
            tenant_session = None
            try:
                tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
                tenant_session = get_tenant_session(tenant_db_url)
                variant_id = data.resourceId
                
                print(f"Iniciando sync_bsale_variant para VariantID: {variant_id}")
                synced_variant = sync_bsale_variant(tenant_session, api_key, variant_id)
                
                if synced_variant:
                    tenant_session.commit()
                    print(f"Webhook 'variant' (VariantID: {variant_id}) procesado y commiteado.")
                else:
                    print(f"sync_bsale_variant para VariantID {variant_id} no devolvió un objeto. Se hará rollback si hubo cambios en sesión.")
                    tenant_session.rollback()
            except Exception as e:
                print(f"ERROR al procesar webhook 'variant' (VariantID: {data.resourceId}): {e}")
                if tenant_session:
                    tenant_session.rollback()
            finally:
                if tenant_session:
                    tenant_session.close()
        else:
            print(f"Tópico de webhook desconocido: {data.topic}")

        print(f"Procesamiento de webhook finalizado para Tópico: {data.topic}")
    
    except Exception as e:
        print("--- ERROR CRÍTICO EN TAREA DE FONDO (procesar_webhook) ---")
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()
        print(f"Procesamiento de webhook y cierre de sesión finalizado para Tópico: {data.topic}")

