from typing import List, Optional
from fastapi import HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from models.main_db import CompanyRole, User
from schemas.user_schemas import UserCreate, UserUpdate, UserUpdateMe
from core.security import get_password_hash, verify_password_reset_token
from models.main_db import Company

def get_user(db: Session, user_id: int):
    return db.query(User).filter(User.id == user_id).first()

def get_user_by_email(db: Session, email: str):
    return db.query(User).filter(User.email == email).first()

def get_users(
    db: Session, 
    text: Optional[str] = None, 
    is_active: Optional[bool] = None, 
    role_filter: Optional[str] = None
) -> List[User]:
    """
    Obtiene una lista de usuarios, aplicando filtros dinámicamente.
    """
    query = db.query(User)

    if is_active is not None:
        query = query.filter(User.is_active == is_active)
    
    if role_filter:
        if role_filter == 'superuser':
            query = query.filter(User.is_superuser == True)
        else:
            try:
                role_enum_member = CompanyRole(role_filter)
                query = query.filter(User.role == role_enum_member)
            except ValueError:
                print(f"Advertencia: Rol de filtro '{role_filter}' no es válido. Ignorando filtro.")
                pass
    
    if text:
        search_term = f"%{text.lower()}%"
        query = query.filter(
            or_(
                func.lower(User.full_name).ilike(search_term),
                func.lower(User.email).ilike(search_term)
            )
        )

    return query.order_by(User.full_name).all()

def create_user(db: Session, user_in: UserCreate):
    hashed_password = get_password_hash(user_in.password)
    db_user = User(
        email=user_in.email,
        hashed_password=hashed_password,
        full_name=user_in.full_name,
        phone_number=user_in.phone_number,
        country=user_in.country,
        city=user_in.city,
        role=user_in.role
    )
    # To associate companies at creation
    if user_in.company_ids:
         companies = db.query(Company).filter(Company.id.in_(user_in.company_ids)).all()
         db_user.companies.extend(companies)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def delete_user(db: Session, user_id: int) -> Optional[User]:
    """
    Elimina un usuario de la base de datos por su ID.
    
    Args:
        db: La sesión de la base de datos.
        user_id: El ID del usuario a eliminar.

    Returns:
        El objeto de usuario que fue eliminado, o None si no se encontró.
    """
    # 1. Busca al usuario que se va a eliminar.
    user_to_delete = db.query(User).filter(User.id == user_id).first()

    # 2. Si el usuario existe, elimínalo y guarda los cambios.
    if user_to_delete:
        db.delete(user_to_delete)
        db.commit()
        print(f"Usuario con ID {user_id} eliminado exitosamente.")
    
    # 3. Devuelve el objeto eliminado (o None si no se encontró).
    # El endpoint que llama a esta función se encargará de devolver un 404 si esto es None.
    return user_to_delete

def update_user_profile(db: Session, db_user: User, user_in: UserUpdateMe) -> User:
    """Actualiza el perfil de un usuario con datos limitados."""
    update_data = user_in.model_dump(exclude_unset=True)
    
    for field, value in update_data.items():
        setattr(db_user, field, value)
        
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def get_company_by_rut(db: Session, rut: str):
    return db.query(Company).filter(Company.rut == rut).first()

def update_password(db: Session, user: User, new_password_hashed: str):
    user.hashed_password = new_password_hashed
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def update_user(db: Session, db_user: User, user_in: UserUpdate) -> User:
    """
    Actualiza un usuario en la base de datos.
    - db_user: El objeto SQLAlchemy del usuario existente.
    - user_in: El objeto Pydantic con los datos a actualizar.
    """
    update_data = user_in.model_dump(exclude_unset=True)
    if "email" in update_data and update_data["email"] != db_user.email:
        existing_user_with_email = get_user_by_email(db, email=update_data["email"])
        if existing_user_with_email and existing_user_with_email.id != db_user.id:
            raise HTTPException(
                status_code=409,
                detail="Ya existe otro usuario con este email."
            )

    for field, value in update_data.items():
        setattr(db_user, field, value)

    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user