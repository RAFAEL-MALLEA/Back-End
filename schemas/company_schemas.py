from pydantic import BaseModel, EmailStr, Field, HttpUrl
from typing import Optional, List

from schemas.warehouse_schemas import WarehouseOut

class IntegrationOut(BaseModel):
    id: int
    name: str
    logo_url: Optional[str] = None
    
    class Config:
        from_attributes = True

class CompanyBase(BaseModel):
    name: str
    rut: Optional[str] = None

class CompanyCreate(CompanyBase):
    pass

class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    rut: Optional[str] = None
    selectedAvatar: Optional[str] = None

class MinimalCompanyResponse(BaseModel):
    id: int
    name: str
    rut: Optional[str] = None
    selectedAvatar: Optional[HttpUrl] = None
    integration: Optional[IntegrationOut] = None

    class Config:
        from_attributes = True

class CompanyResponse(CompanyBase):
    id: int
    name: str
    api_key: Optional[str] = None
    rut: Optional[str] = None
    selectedAvatar: Optional[HttpUrl] = None
    warehouses: List[WarehouseOut] = [] 
    integration: Optional[IntegrationOut] = None
    class Config:
        from_attributes = True

class CompanyIntegrationUpdate(BaseModel):
    integration_id: Optional[int] = Field(None, description="El ID de la nueva integración a la que se conectará la empresa. `null` para desconectar.")
    api_key: Optional[str] = Field(None, description="La API Key para la integración seleccionada.")