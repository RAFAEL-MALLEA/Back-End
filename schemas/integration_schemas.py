from pydantic import BaseModel, Field
from typing import Optional

# Para listar las integraciones disponibles (GET /integrations)
class IntegrationOut(BaseModel):
    id: int
    name: str
    logo_url: Optional[str] = None

    class Config:
        from_attributes = True

# Para el cuerpo de la solicitud PUT /companies/{id}/integration
class CompanyIntegrationUpdate(BaseModel):
    integration_id: Optional[int] = Field(None, description="El ID de la nueva integración a la que se conectará la empresa. `null` para desconectar.")
    api_key: Optional[str] = Field(None, description="La API Key para la integración seleccionada.")