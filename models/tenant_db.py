import enum
import uuid
from sqlalchemy import (Column,Boolean, Date, Integer, String, Numeric, Time, ForeignKey, 
                        DateTime, UniqueConstraint, func, JSON, Enum, Text)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base, relationship

TenantBase = declarative_base()

class Warehouse(TenantBase):
    __tablename__ = "warehouses"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    close_time = Column(Time, nullable=True)
    working_days = Column(JSON, nullable=True)
    open_time = Column(Time, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    transactions = relationship("Transaction", back_populates="warehouse")
    
class Product(TenantBase):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    description = Column(String, nullable=True)
    barcode = Column(String)
    code = Column(String)
    category = Column(String, nullable=True)
    cost = Column(Numeric(10, 2), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Seller(TenantBase):
    """Modelo para los vendedores."""
    __tablename__ = "sellers"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    employee_code = Column(String(50), unique=True, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    transactions = relationship("Transaction", back_populates="seller")

class Transaction(TenantBase):
    """Representa una transacción completa (ej. una boleta, una factura, una guía de ajuste)."""
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    reference_code = Column(String(100), nullable=False, index=True)
    transaction_date = Column(Date, nullable=False, index=True)
    transaction_time = Column(Time(timezone=True), nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    seller_id = Column(Integer, ForeignKey("sellers.id"), nullable=True)
    warehouse = relationship("Warehouse", back_populates="transactions")
    seller = relationship("Seller", back_populates="transactions")
    details = relationship("TransactionDetail", back_populates="transaction", cascade="all, delete-orphan")
    # --- RESTRICCIÓN DE UNICIDAD COMPUESTA ---
    __table_args__ = (
        UniqueConstraint('reference_code', 'warehouse_id', name='_reference_code_warehouse_uc'),
    )

class MovementType(enum.Enum):
    """Define los tipos de movimientos de stock posibles."""
    VENTA = "venta"
    RESERVADO = "reservado"
    RECEPCION = "recepcion"
    DEVOLUCION = "devolucion"
    AJUSTE = "ajuste"
    STOCK_INICIAL = "stock_inicial"
    TRASLADO_ENTRADA = "traslado_entrada"
    TRASLADO_SALIDA = "traslado_salida"

class TransactionDetail(TenantBase):
    """Representa una línea individual dentro de una transacción (un movimiento de stock)."""
    __tablename__ = "transaction_details"
    id = Column(Integer, primary_key=True)
    transaction_id = Column(Integer, ForeignKey("transactions.id", ondelete="CASCADE"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_cost = Column(Numeric(10, 2), nullable=True)
    unit_price = Column(Numeric(10, 2), nullable=True)
    movement_type = Column(Enum(MovementType), nullable=False)
    transaction = relationship("Transaction", back_populates="details")
    product = relationship("Product")

class DailyStockSnapshot(TenantBase):
    __tablename__ = 'daily_stock_snapshots'

    id = Column(Integer, primary_key=True)
    snapshot_date = Column(Date, nullable=False)
    
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    warehouse_id = Column(Integer, ForeignKey('warehouses.id', ondelete="CASCADE"), nullable=False)

    # Stock al inicio del día
    opening_physical_stock = Column(Integer, nullable=False, default=0)
    opening_reserved_stock = Column(Integer, nullable=False, default=0)

    # Movimientos físicos totales del día
    quantity_sold = Column(Integer, nullable=False, default=0)
    quantity_returned = Column(Integer, nullable=False, default=0)
    quantity_purchased = Column(Integer, nullable=False, default=0)
    quantity_adjusted = Column(Integer, nullable=False, default=0)
    quantity_transfer_in = Column(Integer, nullable=False, default=0)
    quantity_transfer_out = Column(Integer, nullable=False, default=0)
    
    # Nuevos campos para rastrear cambios en las reservas durante el día
    quantity_newly_reserved = Column(Integer, nullable=False, default=0)
    quantity_released_from_reservation = Column(Integer, nullable=False, default=0)

    # Estado del stock al final del día
    closing_physical_stock = Column(Integer, nullable=False, default=0)
    closing_reserved_stock = Column(Integer, nullable=False, default=0)
    closing_available_stock = Column(Integer, nullable=False, default=0)
    
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    product = relationship("Product")
    warehouse = relationship("Warehouse")
    
    __table_args__ = (UniqueConstraint('snapshot_date', 'product_id', 'warehouse_id', name='_snapshot_uc'),)

class MetricName(enum.Enum):
    """Define los tipos de movimientos de stock posibles."""
    STOCK_CERO = "stock_cero"
    BAJA_ROTACION = "baja_rotacion"
    STOCK_CRITICO = "stock_critico"
    SOBRE_STOCK = "sobre_stock"
    RECOMENDACION_COMPRA = "recomendacion_compra"
    DEVOLUCIONES = "devoluciones"
    AJUSTE_STOCK = "ajuste"
    VENTA_SIN_STOCK = "venta_sin_stock"

class MetricAlert(TenantBase):
    __tablename__ = "metric_alerts"

    id = Column(Integer, primary_key=True)
    alert_date = Column(Date, nullable=False, index=True)
    metric_name = Column(Enum(MetricName), nullable=False, index=True)
    
    product_id = Column(Integer, ForeignKey('products.id', ondelete="CASCADE"), nullable=False)
    warehouse_id = Column(Integer, ForeignKey('warehouses.id', ondelete="CASCADE"), nullable=False)

    # Datos del snapshot relevantes en el momento de la alerta para referencia y para la API
    physical_stock_at_alert = Column(Integer, nullable=True)
    available_stock_at_alert = Column(Integer, nullable=True)
    reserved_stock_at_alert = Column(Integer, nullable=True)
    
    # Valor específico de la métrica, ej. días de cobertura, cantidad a comprar, días sin venta
    metric_value_numeric = Column(Numeric(12, 2), nullable=True) 
    metric_value_text = Column(String, nullable=True) # Para valores no numéricos

    # JSON para contexto adicional (ej. promedio de ventas usado, umbrales)
    details_json = Column(JSON, nullable=True) 
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    # No necesitamos updated_at aquí si cada alerta es un evento puntual diario.
    # Si una alerta puede cambiar su severidad en el mismo día, un UPSERT la actualizaría.

    product = relationship("Product") # Para acceder a Product.code, Product.name, Product.cost, Product.category
    warehouse = relationship("Warehouse") # Para acceder a Warehouse.name

    # Un producto puede tener múltiples tipos de alerta en el mismo día,
    # o la misma alerta en múltiples días.
    # Esta restricción asegura que para un producto/sucursal/métrica específica, solo haya un registro por día.
    __table_args__ = (UniqueConstraint('alert_date', 'metric_name', 'product_id', 'warehouse_id', name='_alert_uc'),)

class MetricConfiguration(TenantBase):
    """
    Almacena la configuración personalizada para cada métrica.
    Puede ser global para la empresa (warehouse_id es NULL) 
    o específica para una sucursal (warehouse_id tiene un valor).
    """
    __tablename__ = 'metric_configurations'
    
    id = Column(Integer, primary_key=True)
    metric_name = Column(Enum(MetricName), nullable=False)
    
    # Si warehouse_id es NULL, es la configuración global/default para la empresa.
    warehouse_id = Column(Integer, ForeignKey('warehouses.id', ondelete="CASCADE"), nullable=True)
    
    # JSONB es preferible en PostgreSQL para flexibilidad y rendimiento con JSON.
    # Aquí se guardan los parámetros específicos de la métrica, ej:
    # para STOCK_CRITICO: {"days_for_avg": 30, "coverage_days_threshold": 3, "stock_qty_threshold": 3}
    config_json = Column(JSON, nullable=False) 
    
    is_active = Column(Boolean, default=True, nullable=False) # Una configuración puede desactivarse
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('metric_name', 'warehouse_id', name='_metric_name_warehouse_uc'),
    )

class GeneratedReport(TenantBase):
    __tablename__ = 'generated_reports'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    report_date = Column(Date, nullable=False, index=True)
    warehouse_id = Column(Integer, ForeignKey('warehouses.id'), nullable=True)
    report_data = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    warehouse = relationship("Warehouse")

class GeneratedMetricReport(TenantBase):
    __tablename__ = 'generated_metric_reports'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    report_date = Column(Date, nullable=False, index=True)
    warehouse_id = Column(Integer, ForeignKey('warehouses.id'), nullable=True, index=True)
    report_data = Column(JSONB, nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    warehouse = relationship("Warehouse")
    __table_args__ = (UniqueConstraint('report_date', 'warehouse_id', name='_report_date_warehouse_uc'),)
