from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from database.main_db import get_db
from models.main_db import Integration, User as UserModel
from schemas.integration_schemas import IntegrationOut
from auth.auth_bearer import get_current_active_user

router = APIRouter(
    prefix="/integrations",
    tags=["Integrations"]
)

@router.get("", response_model=List[IntegrationOut])
async def get_available_integrations(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Devuelve una lista de todas las integraciones activas disponibles en la plataforma.
    El frontend puede usar esto para poblar un <Select> o una lista de tarjetas.
    """
    return db.query(Integration).filter_by(is_active=True).all()