from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from database.main_db import get_db
from models.main_db import MetricDisplayConfiguration, User as UserModel, Company
from models.tenant_db import MetricName
from schemas.metric_schemas import MetricDisplayCreate, MetricDisplayUpdate, MetricDisplayOut
from auth.auth_bearer import get_current_active_user

router = APIRouter(
    prefix="/metrics/{company_id}/metric-types",
    tags=["Metric Display Configurations"]
)

def get_config_by_id(db: Session, config_id: int, company_id: int):
    return db.query(MetricDisplayConfiguration).filter(
        MetricDisplayConfiguration.id == config_id,
        MetricDisplayConfiguration.company_id == company_id
    ).first()

@router.post("", response_model=MetricDisplayOut, status_code=status.HTTP_201_CREATED)
async def create_metric_display_config(
    company_id: int,
    config_in: MetricDisplayCreate,
    db: Session = Depends(get_db),
):
    """Crea una nueva configuración de visualización para una métrica."""
    existing = db.query(MetricDisplayConfiguration.id).filter_by(
        company_id=company_id, metric_id=config_in.metric_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Ya existe una configuración para esta métrica en esta empresa.")

    db_config = MetricDisplayConfiguration(**config_in.model_dump(), company_id=company_id)
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return db_config

@router.get("", response_model=List[MetricDisplayOut])
async def list_metric_display_configs(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Obtiene todas las configuraciones de visualización de métricas para una empresa."""
    configs = db.query(MetricDisplayConfiguration).filter_by(company_id=company_id).all()
    return configs

@router.put("/{config_id}", response_model=MetricDisplayOut)
async def update_metric_display_config(
    company_id: int,
    config_id: int,
    config_in: MetricDisplayUpdate,
    db: Session = Depends(get_db),
):
    """Actualiza una configuración de visualización de métrica existente."""
    print(config_in)
    db_config = get_config_by_id(db, config_id, company_id)
    if not db_config:
        raise HTTPException(status_code=404, detail="Configuración no encontrada.")

    update_data = config_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_config, key, value)
    
    db.commit()
    db.refresh(db_config)
    return db_config

@router.delete("/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_metric_display_config(
    company_id: int,
    config_id: int,
    db: Session = Depends(get_db),
):
    """Elimina una configuración de visualización de métrica."""
    db_config = get_config_by_id(db, config_id, company_id)
    if not db_config:
        raise HTTPException(status_code=404, detail="Configuración no encontrada.")
    
    db.delete(db_config)
    db.commit()
    return