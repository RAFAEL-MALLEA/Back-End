from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import select, update
from typing import List
from auth.auth_bearer import get_current_active_user
from database.main_db import get_db
from models.main_db import User, Notification, notification_user_association
from schemas.notification import NotificationSchema

router = APIRouter(
    prefix="/notifications",
    tags=["Notifications"]
)
@router.get("/", response_model=List[NotificationSchema])
def get_user_notifications(
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Obtiene todas las notificaciones para el usuario actual.
    
    Esta consulta une la tabla de notificaciones con la de asociación
    para obtener el estado 'is_read' específico del usuario.
    """
    stmt = (
        select(
            Notification.id,
            Notification.message,
            Notification.created_at,
            notification_user_association.c.is_read,
        )
        .join(
            notification_user_association,
            Notification.id == notification_user_association.c.notification_id,
        )
        .where(notification_user_association.c.user_id == current_user.id)
        .order_by(Notification.created_at.desc())
    )

    results = db.execute(stmt).all()
    return results


@router.post("/{notification_id}/read", status_code=status.HTTP_204_NO_CONTENT)
def mark_notification_as_read(
    notification_id: int,
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Marca una notificación como leída para el usuario actual.
    """
    stmt = (
        update(notification_user_association)
        .where(
            notification_user_association.c.user_id == current_user.id,
            notification_user_association.c.notification_id == notification_id,
        )
        .values(is_read=True)
    )

    result = db.execute(stmt)
    db.commit()

    if result.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La notificación no existe o no tienes acceso a ella.",
        )
    
    return