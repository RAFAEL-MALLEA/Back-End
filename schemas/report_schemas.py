from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel
from typing import List, Optional
from datetime import date
from models.tenant_db import MetricName

class ReportBase(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

class OccurrencesAlertOut(ReportBase):
    id: str
    metric_id: MetricName
    date: date
    alert_days: Optional[int] = None

class ProductAlertOut(ReportBase):
    id: str
    name: str
    product_sku: str = Field(..., alias="product")
    store: str
    occurrences: List[OccurrencesAlertOut]

class ReportOut(ReportBase):
    id: str
    init_date: date
    end_date: date
    products: List[ProductAlertOut]

class SingleReportResponse(ReportBase):
    """Esquema para la respuesta que contiene un solo reporte anidado."""
    report: ReportOut

class ReportListOut(ReportBase):
    """
    Este es el objeto principal que se devolverá en la respuesta de la API.
    Contiene una lista de reportes.
    """
    reports: List[ReportOut]