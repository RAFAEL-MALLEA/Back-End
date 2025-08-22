from pydantic import BaseModel
from typing import Any, Optional
from datetime import time, datetime

# Esquema base con los campos comunes
class WarehouseBase(BaseModel):
    name: str
    open_time: Optional[time] = None
    close_time: Optional[time] = None
    working_days: Optional[Any] = None

# Esquema para la creación (POST /warehouses)
# Hereda de WarehouseBase, todos sus campos son requeridos.
class WarehouseCreate(WarehouseBase):
    pass

# Esquema para la actualización (PUT /warehouses/{id})
# Todos los campos son opcionales para permitir actualizaciones parciales (comportamiento tipo PATCH).
class WarehouseUpdate(BaseModel):
    name: Optional[str] = None
    open_time: Optional[time] = None
    close_time: Optional[time] = None
    working_days: Optional[Any] = None

# Esquema para la respuesta (GET /warehouses, y respuestas de POST/PUT)
class WarehouseOut(WarehouseBase):
    id: int
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True