from http.client import HTTPException
from fastapi import APIRouter, BackgroundTasks, Depends, status
from requests import Session
from database.main_db import SessionLocal as MainSessionLocal, get_db
from services.jl_caspana_service import run_jl_caspana_alert_etl

router = APIRouter(
    prefix="/integrations/jl-caspana",
    tags=["Integrations - JL Caspana"]
)

def get_main_db():
    db = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/sync", status_code=status.HTTP_202_ACCEPTED)
async def trigger_jl_caspana_alert_sync(
    background_tasks: BackgroundTasks,
    main_db: Session = Depends(get_db)
):
    """
    Inicia un proceso ETL que consulta las métricas directamente en RP Sistemas
    y guarda el estado de las alertas para el día de hoy en nuestra tabla MetricAlert.
    """
    background_tasks.add_task(
        run_jl_caspana_alert_etl,
        company_id=47,
        main_db=main_db
    )
    
    return {"message": "El proceso de extracción y carga de alertas desde RP Sistemas ha sido iniciado en segundo plano."}