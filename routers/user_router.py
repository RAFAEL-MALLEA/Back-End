from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session
from typing import List, Optional

from core.security import get_password_hash, verify_password
from database.main_db import get_db
from models.main_db import CompanyRole, User
from schemas.user_schemas import UserCreate, UserResponse, UserUpdate, ChangePasswordRequest, MinimalCompanyResponse, UserUpdateMe
from crud import user_crud, company_crud
from auth.auth_bearer import get_current_active_superuser, get_current_active_user
from fastapi_cache.decorator import cache

router = APIRouter(
    prefix="/users",
    tags=["Users"]
)

@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user_by_admin(
    user_in: UserCreate,
    db: Session = Depends(get_db),
    current_admin: User = Depends(get_current_active_superuser) 
):
    """
    Crea un nuevo usuario. Accesible solo para superadministradores.
    Permite asignar compañías y un rol durante la creación.
    """
    db_user = user_crud.get_user_by_email(db, email=user_in.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Un usuario con este email ya existe.")
    
    return user_crud.create_user(db=db, user_in=user_in)

@router.get("", response_model=List[UserResponse])
@cache(expire=300)
async def get_all_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    text: Optional[str] = Query(None, description="Texto para buscar por nombre o email."),
    is_active: Optional[bool] = Query(None, description="Filtrar por estado activo o inactivo."),
    role: Optional[str] = Query(None, description="Filtrar por rol: 'Usuario', 'Administrador de empresa', o 'superuser'")
):
    """
    Obtiene una lista de todos los usuarios, con filtros opcionales.
    """
    users = user_crud.get_users(
        db=db, 
        text=text, 
        is_active=is_active, 
        role_filter=role
    )
    return users

@router.get("/me", response_model=UserResponse)
@cache(expire=300)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """Obtiene el perfil del usuario actual."""
    return current_user

@router.put("/me", response_model=UserResponse)
async def update_users_me(
    user_in: UserUpdateMe,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Actualiza el perfil del usuario actualmente autenticado."""
    updated_user = user_crud.update_user_profile(db=db, db_user=current_user, user_in=user_in)
    return updated_user

@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_by_admin(
    user_id: int,
    db: Session = Depends(get_db),
    current_admin: User = Depends(get_current_active_superuser)
):
    """
    Elimina un usuario. Accesible solo para superadministradores.
    TODO: Añadir lógica para que un admin de empresa pueda borrar solo usuarios de su empresa.
    """
    user_to_delete = user_crud.get_user(db, user_id=user_id)
    if not user_to_delete:
        raise HTTPException(status_code=404, detail="Usuario no encontrado.")
    
    # Regla de negocio: Un usuario no puede eliminarse a sí mismo
    if user_to_delete.id == current_admin.id:
        raise HTTPException(status_code=400, detail="No puedes eliminar tu propia cuenta.")

    user_crud.delete_user(db=db, user_id=user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.get("/{user_id}", response_model=UserResponse)
@cache(expire=300)
async def get_user_by_id(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Obtiene un usuario por su ID."""
    user = user_crud.get_user(db=db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.put("/{user_id}", response_model=UserResponse)
async def update_user_by_admin(
    user_id: int,
    user_in: UserUpdate,
    db: Session = Depends(get_db),
    current_admin: User = Depends(get_current_active_superuser)
):
    """
    Actualiza la información de un usuario, incluido su rol.
    Accesible solo para superadministradores.
    """
    print(user_in)
    db_user = user_crud.get_user(db, user_id=user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado.")
    
    return user_crud.update_user(db=db, db_user=db_user, user_in=user_in)

@router.put("/me/change-password", status_code=status.HTTP_204_NO_CONTENT)
async def change_current_user_password(
    password_data: ChangePasswordRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Change current user's password.
    """
    if not verify_password(password_data.current_password, current_user.hashed_password):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Incorrect current password")
    if password_data.current_password == password_data.new_password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="New password cannot be the same as the old password")

    hashed_new_password = get_password_hash(password_data.new_password)
    user_crud.update_password(db=db, user=current_user, new_password_hashed=hashed_new_password)
    return

@router.get("/me/companies", response_model=List[MinimalCompanyResponse])
@cache(expire=300)
async def get_my_associated_companies(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get all companies associated with the current user.
    """
    return current_user.companies

@router.get("/me/companies/{company_id}", response_model=MinimalCompanyResponse)
@cache(expire=300)
async def get_specific_company_of_mine(
    company_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get a specific company by ID, if it's associated with the current user.
    """
    company = company_crud.get_specific_company_for_user(db=db, user_id=current_user.id, company_id=company_id)
    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found or not associated with user")
    return company