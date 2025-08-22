from sqlalchemy.orm import Session
from sqlalchemy import Date, func, and_, or_, cast, Numeric as SqlAlchemyNumeric, desc
from datetime import date, timedelta
from typing import List, Dict, Any, Optional
from decimal import Decimal

# Importar todos los modelos necesarios de Bsale
from models.bsale_db import (
    Bsale_Document_Type, Bsale_Stock, Bsale_Document_Detail, Bsale_Document, Bsale_Product, Bsale_Variant, 
    Bsale_Office
)
from models.tenant_db import MetricName
from routers.metrics_router import get_consecutive_alert_days
from schemas.metric_schemas import MetricDataItem
from services.bsale_etl_service import get_stock_consumptions_for_date

# --- Helper para construir el MetricDataItem ---
# Para no repetir código en cada función
def _build_metric_data_item(
    stock: Bsale_Stock, 
    variant: Bsale_Variant, 
    product: Bsale_Product, 
    office: Bsale_Office, 
    report_date: date, 
    metric_val: Any = None, 
    alert_d: Optional[int] = None
) -> MetricDataItem:

    price = 0.0 

    return MetricDataItem(
        product=variant.code or str(product.id),
        name=f"{product.name} ({variant.description})" if variant.description else product.name,
        store=office.name,
        loteType=product.product_type.name if product.product_type else "N/A",
        location=str(office.id),
        located=stock.quantity,
        ready=stock.quantity_available,
        blocked=stock.quantity_reserved,
        price=price,
        date=report_date,
        metric_value=metric_val,
        alertDays=alert_d
    )

# --- Funciones de Métricas ---

def get_bsale_stock_cero(
    db: Session,
    end_date: date,
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """
    Obtiene productos con stock disponible <= 0 desde Bsale_Stock y calcula
    los días consecutivos que han estado en alerta.
    """
    query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office
    ).join(Bsale_Variant, Bsale_Stock.variant_id == Bsale_Variant.id)\
     .join(Bsale_Product, Bsale_Variant.product_id == Bsale_Product.id)\
     .join(Bsale_Office, Bsale_Stock.office_id == Bsale_Office.id)\
     .filter(Bsale_Stock.quantity_available <= 0, Bsale_Product.stock_control == True)

    if warehouse_id:
        query = query.filter(Bsale_Office.id.in_(
            [bsale_office_id for bsale_office_id, internal_id in warehouse_map.items() if internal_id == warehouse_id]
        ))

    results = []
    for stock, variant, product, office in query.all():
        product_code = variant.code or variant.bar_code
        if not product_code: continue
        internal_product_info = product_map.get(product_code)
        internal_warehouse_id = warehouse_map.get(office.name)

        if not internal_product_info or not internal_warehouse_id:
            continue
        consecutive_days = get_consecutive_alert_days(
            db=db,
            product_id=internal_product_info['id'],
            warehouse_id=internal_warehouse_id,
            metric_name=MetricName.STOCK_CERO,
            end_date=end_date
        )
        
        item = _build_metric_data_item(
            stock, variant, product, office, end_date, 
            metric_val=0,
            alert_d=consecutive_days
        )
        results.append(item)
        
    return results

def get_bsale_stock_critico(
    db: Session, 
    end_date: date, 
    config: Dict[str, Any], 
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """
    Calcula el stock crítico usando datos de Bsale, incluyendo días de cobertura
    y días consecutivos en alerta.
    """
    SALE_DOCUMENT_TYPES = [36] 
    avg_sales_start_date = end_date - timedelta(days=config["days_for_avg"])

    subquery_avg_sales = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id,
        (func.sum(Bsale_Document_Detail.quantity) / config["days_for_avg"]).label('avg_daily_sales')
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
        cast(Bsale_Document.date, Date).between(avg_sales_start_date, end_date)
    ).group_by(Bsale_Document_Detail.variant_id, Bsale_Document.office_id).subquery()
     
    query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office,
        func.coalesce(subquery_avg_sales.c.avg_daily_sales, 0).label('avg_sales')
    ).select_from(Bsale_Stock)\
     .join(Bsale_Variant, Bsale_Stock.variant_id == Bsale_Variant.id)\
     .join(Bsale_Product, Bsale_Variant.product_id == Bsale_Product.id)\
     .join(Bsale_Office, Bsale_Stock.office_id == Bsale_Office.id)\
     .outerjoin(subquery_avg_sales, and_(
        Bsale_Stock.variant_id == subquery_avg_sales.c.variant_id,
        Bsale_Stock.office_id == subquery_avg_sales.c.office_id
     ))\
     .filter(Bsale_Product.stock_control == True)

    if warehouse_id:
        query = query.filter(Bsale_Stock.office_id == warehouse_id)

    results = []
    for stock, variant, product, office, avg_sales in query.all():
        avg_daily_sales = Decimal(avg_sales)
        available_stock = Decimal(stock.quantity_available)
        
        is_critical = (available_stock <= (avg_daily_sales * Decimal(config["coverage_days_threshold"]))) or \
                      (available_stock <= Decimal(config["stock_qty_threshold"]))
        
        if is_critical:
            days_coverage = available_stock / avg_daily_sales if avg_daily_sales > 0 else 999.0
            product_code = variant.code or variant.bar_code
            internal_product_info = product_map.get(product_code)
            internal_warehouse_id = warehouse_map.get(office.name)

            consecutive_days = 0
            if internal_product_info and internal_warehouse_id:
                consecutive_days = get_consecutive_alert_days(
                    db=db,
                    product_id=internal_product_info['id'],
                    warehouse_id=internal_warehouse_id,
                    metric_name=MetricName.STOCK_CRITICO,
                    end_date=end_date
                )
            
            item = _build_metric_data_item(
                stock, variant, product, office, end_date, 
                metric_val=float(days_coverage), 
                alert_d=consecutive_days
            )
            results.append(item)
            
    return results

def get_bsale_sobre_stock(
    db: Session, 
    end_date: date, 
    config: Dict[str, Any], 
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """Calcula el sobre stock, incluyendo días de cobertura y días consecutivos en alerta."""
    
    SALE_DOCUMENT_NAMES = ["BOLETA ELECTRÓNICA T", "NOTA VENTA", "FACTURA ELECTRÓNICA T","COMPROBANTE DE VENTA"]
    sale_doc_type_ids_tuples = db.query(Bsale_Document_Type.id)\
        .filter(Bsale_Document_Type.name.in_(SALE_DOCUMENT_NAMES)).all()
    
    SALE_DOCUMENT_TYPES = [res[0] for res in sale_doc_type_ids_tuples]
    
    avg_sales_start_date = end_date - timedelta(days=config["days_for_avg"])

    subquery_avg_sales = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id,
        (func.sum(Bsale_Document_Detail.quantity) / config["days_for_avg"]).label('avg_daily_sales')
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
        cast(Bsale_Document.date, Date).between(avg_sales_start_date, end_date)
    ).group_by(Bsale_Document_Detail.variant_id, Bsale_Document.office_id).subquery()
     
    query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office,
        func.coalesce(subquery_avg_sales.c.avg_daily_sales, 0).label('avg_sales')
    ).select_from(Bsale_Stock)\
     .join(Bsale_Variant, Bsale_Stock.variant_id == Bsale_Variant.id)\
     .join(Bsale_Product, Bsale_Variant.product_id == Bsale_Product.id)\
     .join(Bsale_Office, Bsale_Stock.office_id == Bsale_Office.id)\
     .outerjoin(subquery_avg_sales, and_(
        Bsale_Stock.variant_id == subquery_avg_sales.c.variant_id,
        Bsale_Stock.office_id == subquery_avg_sales.c.office_id
     ))\
     .filter(Bsale_Product.stock_control == True)

    if warehouse_id:
        query = query.filter(Bsale_Stock.office_id == warehouse_id)

    results = []
    for stock, variant, product, office, avg_sales in query.all():
        avg_daily_sales = Decimal(avg_sales)
        physical_stock = Decimal(stock.quantity)

        is_overstock = False
        if avg_daily_sales == 0 and physical_stock > 0:
            is_overstock = True
        elif avg_daily_sales > 0 and physical_stock > (avg_daily_sales * Decimal(config["coverage_days_threshold"])):
            is_overstock = True

        if is_overstock:
            days_coverage_physical = physical_stock / avg_daily_sales if avg_daily_sales > 0 else 999.0
            product_code = variant.code or variant.bar_code
            internal_product_info = product_map.get(product_code)
            internal_warehouse_id = warehouse_map.get(office.name)

            consecutive_days = 0
            if internal_product_info and internal_warehouse_id:
                consecutive_days = get_consecutive_alert_days(
                    db=db,
                    product_id=internal_product_info['id'],
                    warehouse_id=internal_warehouse_id,
                    metric_name=MetricName.SOBRE_STOCK,
                    end_date=end_date
                )
            
            item = _build_metric_data_item(
                stock, variant, product, office, end_date, 
                metric_val=float(days_coverage_physical), 
                alert_d=consecutive_days
            )
            results.append(item)
            
    return results

def get_bsale_baja_rotacion(
    db: Session, 
    end_date: date, 
    config: Dict[str, Any],
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """
    Calcula productos de baja rotación, incluyendo días en alerta y promedio de ventas.
    """
    days_since_last_sale_threshold = config["days_since_last_sale"]
    low_rotation_cutoff_date = end_date - timedelta(days=days_since_last_sale_threshold)
    
    avg_sales_start_date = end_date - timedelta(days=30)
    
    SALE_DOCUMENT_NAMES = ["BOLETA ELECTRÓNICA T", "NOTA VENTA", "FACTURA ELECTRÓNICA T","COMPROBANTE DE VENTA"]
    sale_doc_type_ids_tuples = db.query(Bsale_Document_Type.id)\
        .filter(Bsale_Document_Type.name.in_(SALE_DOCUMENT_NAMES)).all()
    
    SALE_DOCUMENT_TYPES = [res[0] for res in sale_doc_type_ids_tuples]

    last_sale_subq = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id,
        func.max(cast(Bsale_Document.date, Date)).label("last_sale_date")
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES)
    ).group_by(Bsale_Document_Detail.variant_id, Bsale_Document.office_id).subquery()

    avg_sales_subq = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id,
        (func.sum(Bsale_Document_Detail.quantity) / 30.0).label('avg_daily_sales')
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
        cast(Bsale_Document.date, Date).between(avg_sales_start_date, end_date)
    ).group_by(Bsale_Document_Detail.variant_id, Bsale_Document.office_id).subquery()

    query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office,
        last_sale_subq.c.last_sale_date,
        func.coalesce(avg_sales_subq.c.avg_daily_sales, 0).label('avg_sales')
    ).select_from(Bsale_Stock)\
     .join(Bsale_Variant, Bsale_Stock.variant_id == Bsale_Variant.id)\
     .join(Bsale_Product, Bsale_Variant.product_id == Bsale_Product.id)\
     .join(Bsale_Office, Bsale_Stock.office_id == Bsale_Office.id)\
     .outerjoin(last_sale_subq, and_(
         Bsale_Stock.variant_id == last_sale_subq.c.variant_id,
         Bsale_Stock.office_id == last_sale_subq.c.office_id
     ))\
     .outerjoin(avg_sales_subq, and_(
         Bsale_Stock.variant_id == avg_sales_subq.c.variant_id,
         Bsale_Stock.office_id == avg_sales_subq.c.office_id
     ))\
     .filter(
         Bsale_Stock.quantity > 0,
         Bsale_Product.stock_control == True,
         or_(
             last_sale_subq.c.last_sale_date == None,
             last_sale_subq.c.last_sale_date < low_rotation_cutoff_date
         )
     )
    
    if warehouse_id:
        query = query.filter(Bsale_Stock.office_id == warehouse_id)

    results = []
    for stock, variant, product, office, last_sale_date_val, avg_sales in query.all():
        days_no_sale = (end_date - last_sale_date_val).days if last_sale_date_val else 999

        product_code = variant.code or variant.bar_code
        internal_product_info = product_map.get(product_code)
        internal_warehouse_id = warehouse_map.get(office.name)

        consecutive_days = 0
        if internal_product_info and internal_warehouse_id:
            consecutive_days = get_consecutive_alert_days(
                db=db,
                product_id=internal_product_info['id'],
                warehouse_id=internal_warehouse_id,
                metric_name=MetricName.BAJA_ROTACION,
                end_date=end_date
            )
        
        item = _build_metric_data_item(
            stock, variant, product, office, end_date, 
            metric_val=days_no_sale, 
            alert_d=consecutive_days
        )
        
        results.append(item)
            
    return results

def get_bsale_recomendacion_compra(
    db: Session, 
    end_date: date, 
    config: Dict[str, Any],
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """Calcula recomendaciones de compra usando datos de Bsale."""
    SALE_DOCUMENT_NAMES = ["BOLETA ELECTRÓNICA T", "NOTA VENTA", "FACTURA ELECTRÓNICA T","COMPROBANTE DE VENTA"]
    sale_doc_type_ids_tuples = db.query(Bsale_Document_Type.id)\
        .filter(Bsale_Document_Type.name.in_(SALE_DOCUMENT_NAMES)).all()
    
    SALE_DOCUMENT_TYPES = [res[0] for res in sale_doc_type_ids_tuples]
    sales_lookback_start_date = end_date - timedelta(days=config["sales_days_for_recommendation"])
    subquery_sales = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id,
        func.sum(Bsale_Document_Detail.quantity).label('total_sales')
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
        cast(Bsale_Document.date, Date).between(sales_lookback_start_date, end_date)
    ).group_by(Bsale_Document_Detail.variant_id, Bsale_Document.office_id).subquery()

    query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office,
        func.coalesce(subquery_sales.c.total_sales, 0).label('total_sales_period')
    ).select_from(Bsale_Stock)\
     .join(Bsale_Variant, Bsale_Stock.variant_id == Bsale_Variant.id)\
     .join(Bsale_Product, Bsale_Variant.product_id == Bsale_Product.id)\
     .join(Bsale_Office, Bsale_Stock.office_id == Bsale_Office.id)\
     .outerjoin(subquery_sales, and_(
        Bsale_Stock.variant_id == subquery_sales.c.variant_id,
        Bsale_Stock.office_id == subquery_sales.c.office_id
     ))\
     .filter(Bsale_Product.stock_control == True)
    
    if warehouse_id:
        query = query.filter(Bsale_Stock.office_id == warehouse_id)

    results = []
    for stock, variant, product, office, total_sales in query.all():
        available_stock = Decimal(stock.quantity_available or 0)
        total_sales_period = Decimal(total_sales)

        if available_stock < total_sales_period:
            units_to_buy = total_sales_period - available_stock

            product_code = variant.code or variant.bar_code
            internal_product_info = product_map.get(product_code)
            internal_warehouse_id = warehouse_map.get(office.name)

            consecutive_days = 0
            if internal_product_info and internal_warehouse_id:
                consecutive_days = get_consecutive_alert_days(
                    db=db,
                    product_id=internal_product_info['id'],
                    warehouse_id=internal_warehouse_id,
                    metric_name=MetricName.RECOMENDACION_COMPRA,
                    end_date=end_date
                )
            
            item = _build_metric_data_item(
                stock, variant, product, office, end_date, 
                metric_val=float(units_to_buy), 
                alert_d=consecutive_days
            )
            results.append(item)
            
    return results

def get_bsale_venta_sin_stock(
    db: Session, 
    end_date: date,
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """
    Identifica productos que se vendieron hoy y cuyo stock disponible actual es <= 0.
    """
    SALE_DOCUMENT_NAMES = ["BOLETA ELECTRÓNICA T", "NOTA VENTA", "FACTURA ELECTRÓNICA T","COMPROBANTE DE VENTA"]
    sale_doc_type_ids_tuples = db.query(Bsale_Document_Type.id)\
        .filter(Bsale_Document_Type.name.in_(SALE_DOCUMENT_NAMES)).all()
    
    SALE_DOCUMENT_TYPES = [res[0] for res in sale_doc_type_ids_tuples]
    
    sold_today_query = db.query(
        Bsale_Document_Detail.variant_id,
        Bsale_Document.office_id
    ).join(Bsale_Document).filter(
        Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
        cast(Bsale_Document.date, Date) == end_date
    ).distinct()
    
    if warehouse_id:
        sold_today_query = sold_today_query.filter(Bsale_Document.office_id == warehouse_id)
        
    products_sold_today = { (variant_id, office_id) for variant_id, office_id in sold_today_query.all() }

    if not products_sold_today:
        return []

    zero_stock_query = db.query(
        Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office
    ).join(Bsale_Variant).join(Bsale_Product).join(Bsale_Office)\
     .filter(Bsale_Stock.quantity_available <= 0, Bsale_Product.stock_control == True)
    
    if warehouse_id:
        zero_stock_query = zero_stock_query.filter(Bsale_Stock.office_id == warehouse_id)

    products_with_zero_stock = zero_stock_query.all()
    
    results = []
    for stock, variant, product, office in products_with_zero_stock:
        if (variant.id, office.id) in products_sold_today:
            product_code = variant.code or variant.bar_code
            internal_product_info = product_map.get(product_code)
            internal_warehouse_id = warehouse_map.get(office.name)

            consecutive_days = 0
            if internal_product_info and internal_warehouse_id:
                consecutive_days = get_consecutive_alert_days(
                    db=db,
                    product_id=internal_product_info['id'],
                    warehouse_id=internal_warehouse_id,
                    metric_name=MetricName.VENTA_SIN_STOCK,
                    end_date=end_date
                )
            
            item = _build_metric_data_item(
                stock, variant, product, office, end_date, 
                metric_val=float(stock.quantity_available),
                alert_d=consecutive_days
            )
            results.append(item)
            
    return results

def get_bsale_devoluciones(db: Session, start_date: date, end_date: date, warehouse_id: Optional[int]) -> List[MetricDataItem]:
    """Obtiene las devoluciones (notas de crédito) de Bsale."""
    RETURN_DOCUMENT_TYPES = [60, 61]

    query = db.query(
        Bsale_Document_Detail, Bsale_Document, Bsale_Variant, Bsale_Product, Bsale_Office
    ).select_from(Bsale_Document_Detail)\
     .join(Bsale_Document)\
     .join(Bsale_Variant)\
     .join(Bsale_Product)\
     .join(Bsale_Office, Bsale_Document.office_id == Bsale_Office.id)\
     .filter(
         cast(Bsale_Document.date, Date).between(start_date, end_date),
         Bsale_Document.document_type_id.in_(RETURN_DOCUMENT_TYPES)
     )
    
    if warehouse_id:
        query = query.filter(Bsale_Document.office_id == warehouse_id)
        
    results = []
    for detail, doc, variant, product, office in query.all():
        # Para devoluciones, el stock actual es contextual. Lo obtenemos de Bsale_Stock.
        current_stock = db.query(Bsale_Stock).filter_by(variant_id=variant.id, office_id=office.id).first()
        if not current_stock: # Si no hay registro de stock, crear uno temporal para la respuesta
            current_stock = Bsale_Stock(quantity=0, quantity_available=0, quantity_reserved=0)

        results.append(_build_metric_data_item(current_stock, variant, product, office, cast(doc.date, Date), metric_val=detail.quantity))

    return results

def get_bsale_ajuste_stock(
    db: Session,
    api_key: str,
    start_date: date,
    end_date: date,
    warehouse_map: Dict[str, int],
    product_map: Dict[str, Dict],
    warehouse_id: Optional[int] = None
) -> List[MetricDataItem]:
    """
    Obtiene los ajustes de stock (consumos) desde la API de Bsale para un rango de fechas.
    """
    results = []
    
    # Pre-cargar todos los stocks y variantes para evitar N+1 queries
    all_stock_records = db.query(Bsale_Stock, Bsale_Variant, Bsale_Product, Bsale_Office)\
        .join(Bsale_Variant).join(Bsale_Product).join(Bsale_Office).all()
    
    # Crear mapas para búsqueda rápida
    stock_map_by_variant_office = {(s.variant_id, s.office_id): s for s, v, p, o in all_stock_records}
    variant_map_by_id = {v.id: (v, p, o) for s, v, p, o in all_stock_records}

    # Iterar por cada día en el rango del reporte
    current_date = start_date
    while current_date <= end_date:
        print(f"Buscando ajustes de stock para la fecha: {current_date}...")
        consumptions_for_day = get_stock_consumptions_for_date(api_key, current_date)

        for consumption in consumptions_for_day:
            office_id_bsale = int(consumption.get("office", {}).get("id", 0))
            if warehouse_id and warehouse_map.get(office.name) != warehouse_id:
                 continue

            for detail in consumption.get("details", {}).get("items", []):
                variant_id = int(detail.get("variant", {}).get("id", 0))
                if not variant_id: continue

                variant_data = variant_map_by_id.get(variant_id)
                if not variant_data: continue
                
                variant, product, office = variant_data
                product_code = variant.code or variant.bar_code
                
                internal_product_info = product_map.get(product_code)
                internal_warehouse_id = warehouse_map.get(office.name)
                
                if not internal_product_info or not internal_warehouse_id:
                    continue

                consecutive_days = get_consecutive_alert_days(
                    db=db,
                    product_id=internal_product_info['id'],
                    warehouse_id=internal_warehouse_id,
                    metric_name=MetricName.AJUSTE_STOCK,
                    end_date=current_date
                )
                
                current_stock = stock_map_by_variant_office.get((variant.id, office.id))
                if not current_stock:
                    current_stock = Bsale_Stock(quantity=0, quantity_available=0, quantity_reserved=0)
                
                quantity_adjusted = Decimal(detail.get("quantity", 0))

                item = _build_metric_data_item(
                    current_stock, variant, product, office, current_date, 
                    metric_val=float(quantity_adjusted), 
                    alert_d=consecutive_days
                )
                results.append(item)
        
        current_date += timedelta(days=1)
        
    return results