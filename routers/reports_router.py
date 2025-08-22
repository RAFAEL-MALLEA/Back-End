import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Dict, Tuple
from collections import defaultdict
from datetime import date
from database.main_db import get_db as get_main_db
from database.dynamic import get_tenant_session
from models.main_db import Company, User as UserModel
from models.tenant_db import GeneratedReport, MetricAlert, Product, Warehouse, MetricName
from auth.auth_bearer import get_current_active_user
from schemas.metric_schemas import MetricsApiRequest 
from schemas.report_schemas import ReportListOut, ReportOut, ProductAlertOut, OccurrencesAlertOut, SingleReportResponse
from services.daily_report_service import generate_and_email_daily_report
from services.report_generator_service import generate_and_save_daily_report
from fastapi_cache.decorator import cache

# --- Creación del Router ---
router = APIRouter(
    prefix="/reports",
    tags=["Reports"]
)

@router.post("/{company_id}/generate-today", status_code=status.HTTP_202_ACCEPTED)
async def trigger_daily_report_generation(
    company_id: int,
    background_tasks: BackgroundTasks,
    store: Optional[str] = Query(None, description="Opcional: generar el reporte para una sucursal específica"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Inicia la generación del reporte de alertas para el día de hoy en segundo plano.
    """
    company = main_db.query(Company).filter(Company.id == company_id).first()
    
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)
    target_warehouse_id = None
    if store:
        pass
    tenant_session.close()

    background_tasks.add_task(
        generate_and_save_daily_report,
        db_url=tenant_db_url,
        for_date=date.today(),
        company_id=company_id,
        warehouse_id=target_warehouse_id
    )
    return {"message": "La generación del reporte para hoy ha sido iniciada en segundo plano."}

@router.get("/{company_id}", response_model=ReportListOut, summary="Obtener todos los reportes generados")
@cache(expire=300)
async def list_generated_reports(
    company_id: int,
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Obtiene una lista de todos los reportes que han sido generados y guardados
    previamente para una compañía.
    """
    try:
        company = main_db.query(Company).filter(Company.id == company_id).first()
        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_session = get_tenant_session(tenant_db_url)
        saved_reports = tenant_session.query(GeneratedReport).order_by(GeneratedReport.report_date.desc()).all()
        reports_data = [ReportOut.model_validate(r.report_data) for r in saved_reports]
        
        return ReportListOut(reports=reports_data)
    finally:
        if tenant_session: tenant_session.close()

@router.get("/{company_id}/report/{report_id}", response_model=SingleReportResponse, summary="Obtener un reporte específico")
@cache(expire=300)
async def get_specific_report(
    company_id: int,
    report_id: uuid.UUID,
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """Obtiene un reporte guardado por su ID único."""

    try:
        company = main_db.query(Company).filter(Company.id == company_id).first()
        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_session = get_tenant_session(tenant_db_url)
        db_report = tenant_session.query(GeneratedReport).filter(GeneratedReport.id == report_id).first()
        if not db_report:
            raise HTTPException(status_code=404, detail="Reporte no encontrado.")
        report_data = ReportOut.model_validate(db_report.report_data)
        return SingleReportResponse(report=report_data)
    finally:
        if tenant_session: tenant_session.close()

@router.post("/{company_id}/send-daily-summary", status_code=status.HTTP_202_ACCEPTED)
async def trigger_daily_report_email(
    company_id: int,
    background_tasks: BackgroundTasks,
    main_db: Session = Depends(get_main_db)
):
    """
    Inicia la generación y envío del reporte diario de alertas por correo.
    Esta es una tarea de fondo y la respuesta es inmediata.
    """
    
    background_tasks.add_task(
        generate_and_email_daily_report,
        company_id=company_id,
        main_db_session=main_db
    )
    
    return {"message": "El proceso de generación y envío del reporte diario ha comenzado en segundo plano."}