from decimal import Decimal
from pydantic import BaseModel, Field, field_validator
from datetime import date, time
from typing import Annotated, Any, Literal, Optional
from dateutil import parser

MovementTypeStr = Literal["venta", "reservado", "recepcion", "devolucion", "ajuste", "traslado_entrada", "traslado_salida","stock_inicial"]

class TransactionItemPayload(BaseModel):
    sucursal: str = Field(..., description="Nombre de la sucursal")
    transaccion: str = Field(..., description="Código de referencia de la transacción")
    sku: str = Field(..., description="SKU o código del producto")
    descripcion: str
    tipo: MovementTypeStr
    categoria: Optional[str] = None
    cantidad: int
    costo: Optional[Annotated[Decimal, Field(max_digits=10, decimal_places=2)]] = None
    fecha_movimiento: date
    
    @field_validator('fecha_movimiento', mode='before')
    @classmethod
    def parse_flexible_date(cls, v: Any) -> date:
        if isinstance(v, str):
            try:
                return parser.parse(v).date()
            except parser.ParserError:
                raise ValueError(f"Formato de fecha inválido: '{v}'")
        if isinstance(v, date):
            return v
        raise ValueError(f"Se esperaba un string o un objeto de fecha, se recibió {type(v)}")