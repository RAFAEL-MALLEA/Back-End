from pydantic import BaseModel, Field,ConfigDict
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from models.tenant_db import MetricName
from pydantic.alias_generators import to_camel

# --- Para la sección "overview" ---
class OverviewDataPoint(BaseModel):
    date: date
    net: float       # Valor para la serie de barras (ej. Stock Total $)
    deviation: float # Valor para la serie de líneas (ej. Stock en Alerta $)

class OverviewResponse(BaseModel):
    barMetric: str = "Stock Total Físico $" # Nombre de la métrica para barras
    lineMetric: str = "Stock en Alerta (Crítico) $" # Nombre de la métrica para línea
    data: List[OverviewDataPoint]

# --- Para la sección "grid" ---
class MetricDataItem(BaseModel):
    product_sku: str = Field(..., alias="product")
    product_name: str = Field(..., alias="name")
    store_name: str = Field(..., alias="store")
    category: Optional[str] = Field(None, alias="loteType")
    
    physical_stock: Optional[int] = Field(None, alias="located")
    reserved_stock: Optional[int] = Field(None, alias="blocked")
    available_stock: Optional[int] = Field(None, alias="ready")
    
    metric_value: Optional[Any] = None 
    alert_days: Optional[int] = Field(None, alias="alertDays")
    alert_level: Optional[int] = Field(None, alias="alertLevel")

    cost: Optional[float] = Field(None, alias="price")
    snapshot_date: date = Field(..., alias="date")
    class Config:
        populate_by_name = True

class MetricGridItem(BaseModel):
    name: str
    metricId: MetricName 
    data: List[MetricDataItem] = []

# --- Para la sección "counters" ---
class CounterItem(BaseModel):
    name: str
    quantity: int
    information: str
    backgroundColor: str = "#ffffff"
    text1Color: str = "#1749b7"
    text2Color: str = "#6e6e6e"
    amount: Optional[int] = Field(None, alias="amount") # Cantidad unitaria afectada
    price: Optional[float] = Field(None, alias="price") # Total afectado

# --- Para la sección "distributions" ---
class DistributionItem(BaseModel): # Asumo que es similar a CounterItem para la data
    name: str
    price: float # Valor total para esta categoría de métrica

# --- Para la sección "products" (Escalamiento) ---
class EscalationProductItem(BaseModel):
    name: str # product_name
    alertDays: int

class EscalationItem(BaseModel):
    title: str
    name: str
    abbr: str
    operation: str
    value: int # Umbral para escalamiento, ej. > 3 días
    quantity: int # Cantidad de productos que cumplen la condición de escalamiento
    information: str
    background3Color: str
    text3Color: str
    background4Color: str
    text4Color: str
    amountAlert: int # Podría ser lo mismo que quantity o un subconjunto
    products: List[EscalationProductItem] = []

# --- Esquema Principal de Respuesta ---
class MetricsApiResponse(BaseModel):
    grid: List[MetricGridItem] = []
    overview: Optional[OverviewResponse] = None
    counters: List[CounterItem] = []
    distributions: List[DistributionItem] = []
    products: List[EscalationItem] = [] # Para los "Productos en Escalamiento"

# --- Esquema para los Parámetros de Solicitud del Reporte ---
class MetricsApiRequest(BaseModel):
    dateInit: date
    dateEnd: date
    store: Optional[str] = None


class MetricConfigurationBase(BaseModel):
    metric_name: MetricName
    config_json: Dict[str, Any] = Field(..., description="JSON con los parámetros de la métrica")
    warehouse_id: Optional[int] = None
    is_active: bool = True

class MetricConfigurationCreate(MetricConfigurationBase):
    pass # Todos los campos de Base son necesarios para crear/actualizar por lógica de negocio

class MetricConfigurationUpdate(BaseModel): # Para PUT, permitiendo actualizaciones parciales
    config_json: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None

class MetricConfigurationOut(MetricConfigurationBase):
    id: int
    updated_at: datetime # Añadido para la respuesta

    class Config:
        from_attributes = True

class MetricDisplayCamelBase(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

class MetricDisplayBase(MetricDisplayCamelBase):
    metric_id: MetricName
    name: str = Field(max_length=100)
    title: str = Field(max_length=150)
    short_name: str
    short_name_2: str
    short_name_3: str
    short_name_4: str
    abbreviation: Optional[str] = Field(None, max_length=50)
    information_1: Optional[str] = None
    information_2: Optional[str] = None
    
    show_counter: bool = True
    show_grid: bool = True
    show_distribution: bool = True
    show_products: bool = True
    
    product_operation: str = Field("greater", max_length=20)
    product_number: int = 3
    
    background_color_1: Optional[str] = Field(None, max_length=7)
    text_color_1: Optional[str] = Field(None, max_length=7)
    text_color_2: Optional[str] = Field(None, max_length=7)
    background_color_3: Optional[str] = Field(None, max_length=7)
    text_color_3: Optional[str] = Field(None, max_length=7)
    background_color_4: Optional[str] = Field(None, max_length=7)
    text_color_4: Optional[str] = Field(None, max_length=7)


class MetricDisplayCreate(MetricDisplayBase):
    pass
    
class MetricDisplayUpdate(MetricDisplayCamelBase):
    """
    Esquema para actualizar una configuración de visualización.
    Todos los campos son opcionales para permitir actualizaciones parciales.
    """
    # Textos y Títulosf
    name: Optional[str] = Field(None, max_length=100)
    title: Optional[str] = Field(None, max_length=150)
    short_name: Optional[str] = None
    short_name_2: Optional[str] = None
    short_name_3: Optional[str] = None
    short_name_4: Optional[str] = None
    abbreviation: Optional[str] = Field(None, max_length=50)
    information_1: Optional[str] = None
    information_2: Optional[str] = None
    
    # Visibilidad de Secciones
    show_counter: Optional[bool] = None
    show_grid: Optional[bool] = None
    show_distribution: Optional[bool] = None
    show_products: Optional[bool] = None # El alias 'showProducts' se hereda de la config

    # Parámetros de Escalamiento
    product_operation: Optional[str] = Field(None, max_length=20)
    product_number: Optional[int] = None
    
    # Colores
    background_color_1: Optional[str] = Field(None, max_length=7)
    text_color_1: Optional[str] = Field(None, max_length=7)
    text_color_2: Optional[str] = Field(None, max_length=7)
    background_color_3: Optional[str] = Field(None, max_length=7)
    text_color_3: Optional[str] = Field(None, max_length=7)
    background_color_4: Optional[str] = Field(None, max_length=7)
    text_color_4: Optional[str] = Field(None, max_length=7)

class MetricDisplayOut(MetricDisplayBase):
    id: int
    company_id: int