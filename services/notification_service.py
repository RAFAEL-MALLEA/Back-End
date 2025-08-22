from typing import List
from sqlalchemy.orm import Session
from models.main_db import Notification, User, Company

def create_notification(
    db: Session,
    message: str,
    company: Company,
    users_to_notify: List[User]
) -> Notification:
    """
    Crea una notificación en la base de datos y la asocia a una empresa
    y a una lista de usuarios.
    """
    # 1. Crear la instancia de la notificación
    new_notification = Notification(
        message=message
    )

    # 2. Asociar la notificación con la empresa y los usuarios
    # SQLAlchemy se encargará de poblar las tablas de asociación
    new_notification.companies.append(company)
    new_notification.users.extend(users_to_notify)

    # 3. Guardar en la base de datos
    db.add(new_notification)
    db.commit()
    db.refresh(new_notification)
    
    return new_notification


def create_notification_for_superusers(db: Session, message: str, company_id: int):
    """Crea una notificación y la asocia a todos los superusuarios."""
    superusers = db.query(User).filter(User.is_superuser == True).all()
    if not superusers:
        print("No se encontraron superusuarios para notificar.")
        return

    new_notification = Notification(message=message)
    for user in superusers:
        new_notification.users.append(user)

    db.add(new_notification)
    db.commit()
    print(f"Notificación creada para {len(superusers)} superusuarios.")