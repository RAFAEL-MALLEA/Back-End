from pydantic import BaseModel, ConfigDict, EmailStr, Field
from typing import Optional, List
from pydantic.alias_generators import to_camel
from models.main_db import CompanyRole
from schemas.company_schemas import MinimalCompanyResponse

class UserCamelBase(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

class UserUpdateMe(UserCamelBase):
    """Schema para que un usuario actualice su propio perfil. Campos limitados."""
    full_name: Optional[str] = Field(None, max_length=100)
    phone_number: Optional[str] = Field(None, max_length=30)
    country: Optional[str] = Field(None, max_length=100)
    city: Optional[str] = Field(None, max_length=100)

class UserBase(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    phone_number: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None

class UserCreate(UserBase):
    password: str = Field(min_length=8)
    company_ids: Optional[List[int]] = Field(default_factory=list, description="Lista de IDs de compañías para asociar inicialmente.")
    is_active: bool = True
    is_superuser: bool = False
    role: Optional[CompanyRole] = None

class UserRegister(UserBase):
    password: str = Field(min_length=8)
    company_id: Optional[str] = Field(None, description="ID de la compañía a la que el usuario se unirá (opcional).")

class UserUpdate(BaseModel):
    """Esquema para que un admin actualice los datos de otro usuario."""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    phone_number: Optional[str] = None
    is_superuser: Optional[bool] = None
    role: Optional[CompanyRole] = None

class UserResponse(UserBase):
    id: int
    is_active: bool
    is_superuser: bool
    companies: List[MinimalCompanyResponse] = Field(default_factory=list)
    role: CompanyRole
    class Config:
        from_attributes = True

class UserInDB(UserBase):
    id: int
    hashed_password: str
    is_active: bool
    is_superuser: bool

    class Config:
        from_attributes = True

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8)