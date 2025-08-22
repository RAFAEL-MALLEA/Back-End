from decimal import Decimal
import traceback
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import Date, and_, cast, func, or_, text
from typing import Any, Dict, List, Optional
from datetime import date, datetime, time, timedelta
import time as time_module
from sqlalchemy.dialects import postgresql

# Modelos de la DB del inquilino (ambos esquemas)
from database.dynamic import get_tenant_session
from database.main_db import SessionLocal
from models.tenant_db import Product, Warehouse, MetricAlert, MetricName
from models.bsale_db import Bsale_Document, Bsale_Document_Detail, Bsale_Document_Type, Bsale_Office, Bsale_Price_List_Detail, Bsale_Return, Bsale_Return_Detail, Bsale_Stock, Bsale_Variant, Bsale_Product

# Helper para la conexión y UPSERT
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models.main_db import Company
from routers.metrics_router import get_consecutive_alert_days
from schemas.metric_schemas import MetricDataItem
from services.bsale_api_service import get_bsale_data
from services.get_config import get_effective_metric_config
from utils.bsale.fetch_api import get_bsale

def get_stock_consumptions_for_date(api_key: str, target_date: date) -> List[Dict[str, Any]]:
    """
    Obtiene todos los "consumos de stock" (ajustes) de Bsale para una fecha específica.
    Maneja la paginación para devolver todos los resultados.
    """
    # Convertir la fecha a un timestamp Unix para el inicio del día
    start_of_day = datetime.combine(target_date, time.min)
    unix_timestamp = int(time_module.mktime(start_of_day.timetuple()))
    
    # Endpoint inicial
    endpoint = f"/stocks/consumptions.json?consumptiondate={unix_timestamp}&expand=details"
    all_consumptions = []

    while endpoint:
        print(f"Llamando a la API de Bsale: {endpoint}")
        response_data = get_bsale_data(api_key, endpoint)
        
        if not response_data or not isinstance(response_data, dict):
            print("  -> No se recibieron datos válidos o se alcanzó el final.")
            break
        
        items = response_data.get("items", [])
        if items:
            all_consumptions.extend(items)
        
        endpoint = response_data.get("next")
        if endpoint:
            endpoint = "/" + endpoint.split(".com/", 1)[1]
        
    print(f"Se encontraron {len(all_consumptions)} cabeceras de consumo de stock en total.")
    return all_consumptions

def get_returns_for_date(api_key: str, target_date: date) -> List[Dict[str, Any]]:
    """
    Obtiene todas las devoluciones de Bsale para una fecha específica.
    Maneja la paginación para devolver todos los resultados.
    """
    # Convertir la fecha a un timestamp Unix
    start_of_day = datetime.combine(target_date, time.min)
    unix_timestamp = int(time_module.mktime(start_of_day.timetuple()))
    
    endpoint = f"/returns.json?returndate={unix_timestamp}&expand=details&limit=100"
    all_returns = []

    while endpoint:
        print(f"Llamando a la API de Bsale (Devoluciones): {endpoint}")
        response_data = get_bsale_data(api_key, endpoint)
        
        if not response_data or not isinstance(response_data, dict):
            break
        
        items = response_data.get("items", [])
        if items:
            all_returns.extend(items)
        
        # Obtener la URL de la siguiente página
        endpoint = response_data.get("next")
        if endpoint:
            endpoint = "/" + endpoint.split(".com/", 1)[1]
        
    print(f"Se encontraron {len(all_returns)} cabeceras de devoluciones en total.")
    return all_returns

def _sync_offices_to_warehouses(tenant_db: Session) -> Dict[str, int]:
    """
    Lee las oficinas de Bsale y crea/actualiza registros en la tabla Warehouse.
    Devuelve un mapa de {nombre_oficina: warehouse_id} para uso posterior.
    """
    print("Sincronizando Bsale Offices -> Warehouses...")
    bsale_offices = tenant_db.query(Bsale_Office).all()
    existing_warehouses = {w.name: w.id for w in tenant_db.query(Warehouse).all()}
    new_warehouses_count = 0
    for office in bsale_offices:
        if office.name not in existing_warehouses:
            new_warehouse = Warehouse(name=office.name)
            tenant_db.add(new_warehouse)
            new_warehouses_count += 1
    
    if new_warehouses_count > 0:
        tenant_db.commit()
        print(f"Se crearon {new_warehouses_count} nuevas sucursales.")
        existing_warehouses = {w.name: w.id for w in tenant_db.query(Warehouse).all()}

    return existing_warehouses

def _sync_variants_to_products(tenant_db: Session) -> Dict[str, Dict]:
    """
    Lee las variantes de Bsale y crea/actualiza registros en la tabla Product.
    Si el costo es 0/nulo, intenta usar un precio de una lista de precios como fallback.
    """
    print("Iniciando sincronización de Bsale Variants -> Products...")
    
    all_price_details = tenant_db.query(Bsale_Price_List_Detail).all()
    prices_by_variant_id = defaultdict(list)
    for detail in all_price_details:
        if detail.variant_value > 0:
            prices_by_variant_id[detail.variant_id].append(detail.variant_value)
    
    print(f"Se cargaron precios para {len(prices_by_variant_id)} variantes.")

    bsale_variants = tenant_db.query(Bsale_Variant).join(Bsale_Product)\
        .filter().all()
    
    existing_products_map = {p.code: p for p in tenant_db.query(Product).all()}
    
    created_count = 0
    updated_count = 0
    for variant in bsale_variants:
        product_code = variant.code or variant.bar_code
        if not product_code:
            continue
            
        product = existing_products_map.get(product_code)
        full_name = f"{variant.product.name} ({variant.description})" if variant.description else variant.product.name
        category = variant.product.product_type.name if variant.product.product_type else None

        product_cost = Decimal(0)
        if product_cost <= 0:
            variant_prices = prices_by_variant_id.get(variant.id)
            if variant_prices:
                product_cost = Decimal(variant_prices[0])

        if not product:
            product = Product(
                code=product_code,
                name=full_name,
                category=category,
                cost=product_cost
            )
            tenant_db.add(product)
            created_count += 1
        else:
            if (product.name != full_name or 
                product.category != category or
                product.cost != product_cost):
                
                product.name = full_name
                product.category = category
                product.cost = product_cost
                updated_count += 1
    
    if created_count > 0 or updated_count > 0:
        tenant_db.commit()
    
    print(f"Productos sincronizados. Creados: {created_count}, Actualizados: {updated_count}.")
    return {p.code: {"id": p.id, "cost": p.cost or 0} for p in tenant_db.query(Product).all()}


def _create_metric_alerts(
    tenant_db: Session,
    alert_data_list: List[Dict[str, Any]], # Contiene {"code": "P1", "wh_id": 1 (de Bsale), "value": 10}
    product_map: Dict[str, Dict[str, Any]],
    stock_map: Dict[str, int],
    metric_name: MetricName,
    sync_date: date
):
    """
    Toma una lista de datos de alerta y crea los registros de MetricAlert,
    mapeando el ID de la sucursal de Bsale al ID interno correcto a través del nombre.
    """
    if not alert_data_list:
        return

    # --- PASO 1: Crear un mapa de traducción de IDs ---
    
    # Obtener todos los IDs de sucursales de Bsale únicos del lote de alertas
    bsale_office_ids = {item['wh_id'] for item in alert_data_list if 'wh_id' in item}

    if not bsale_office_ids:
        print("No se encontraron IDs de sucursal en los datos de alerta para procesar.")
        return

    # Obtener los nombres de esas sucursales de Bsale
    bsale_offices = tenant_db.query(Bsale_Office.id, Bsale_Office.name).filter(
        Bsale_Office.id.in_(bsale_office_ids)
    ).all()
    bsale_id_to_name_map = {office.id: office.name for office in bsale_offices}

    # Obtener los IDs de nuestras sucursales internas que coincidan por nombre
    office_names = list(bsale_id_to_name_map.values())
    internal_warehouses = tenant_db.query(Warehouse.id, Warehouse.name).filter(
        Warehouse.name.in_(office_names)
    ).all()
    name_to_internal_id_map = {wh.name: wh.id for wh in internal_warehouses}

    # Crear el mapa de traducción final: {bsale_office_id: internal_warehouse_id}
    translation_map: Dict[int, int] = {
        bsale_id: name_to_internal_id_map.get(bsale_name)
        for bsale_id, bsale_name in bsale_id_to_name_map.items()
        if name_to_internal_id_map.get(bsale_name) is not None
    }
    
    # --- PASO 2: Construir los payloads usando el mapa de traducción ---
    
    payloads_to_upsert = []
    for item in alert_data_list:
        code = item.get("code")
        bsale_wh_id = item.get("wh_id")

        product_info = product_map.get(code)
        
        # Traducir el ID de la sucursal de Bsale al ID interno
        internal_warehouse_id = translation_map.get(bsale_wh_id)

        # Si no encontramos el producto o la traducción de la sucursal, saltamos esta alerta
        if not product_info or not internal_warehouse_id:
            print(metric_name)
            print(f"ADVERTENCIA: Saltando alerta para producto '{code}'. No se encontró el producto o el mapeo para la sucursal Bsale ID {bsale_wh_id}.")
            continue
            
        payload = {
            "alert_date": sync_date,
            "metric_name": metric_name, # Pasamos el objeto Enum directamente
            "product_id": product_info["id"],
            "warehouse_id": internal_warehouse_id, # <-- Usamos el ID interno correcto
            "physical_stock_at_alert": stock_map.get(code, 0),
            "available_stock_at_alert": stock_map.get(code, 0),
            "metric_value_numeric": float(item["value"]) if item.get("value") is not None else None,
        }
        payloads_to_upsert.append(payload)
    
    if not payloads_to_upsert:
        return

    # --- PASO 3: Ejecutar la inserción masiva (sin cambios) ---
    
    stmt = pg_insert(MetricAlert).values(payloads_to_upsert)
    update_dict = {
        "physical_stock_at_alert": stock_map.get(code, 0),
        "available_stock_at_alert": stmt.excluded.available_stock_at_alert,
        "metric_value_numeric": stmt.excluded.metric_value_numeric,
    }
    final_stmt = stmt.on_conflict_do_update(
        index_elements=['alert_date', 'metric_name', 'product_id', 'warehouse_id'],
        set_=update_dict
    )
    
    try:
        tenant_db.execute(final_stmt)
        print(f"  -> Se insertaron/actualizaron {len(payloads_to_upsert)} alertas para la métrica '{metric_name.name}'.")
    except Exception as e:
        print(f"ERROR durante la ejecución de UPSERT para la métrica '{metric_name.name}': {e}")
        raise

def get_all_stocks(api_key: str) -> List[Dict[str, Any]]:
    """
    Obtiene todos los registros de stock de Bsale, manejando la paginación.
    """
    endpoint = "/v1/stocks.json?expand=[variant,office]"
    all_stock_items = get_bsale(api_key,endpoint)
    return all_stock_items

def sync_bsale_stock_levels(tenant_db: Session, api_key: str):
    """
    Obtiene todos los registros de stock de la API de Bsale y los sincroniza
    (inserta o actualiza) en la tabla local Bsale_Stock, validando
    la existencia de las claves foráneas.
    """
    print("Iniciando sincronización de niveles de stock...")
    
    # 1. Obtener todos los stocks de la API (sin cambios)
    all_stocks_from_api = get_all_stocks(api_key)
    if not all_stocks_from_api:
        print("No se encontraron stocks en la API de Bsale para sincronizar.")
        return

    # 2. Obtener datos locales existentes para una sincronización eficiente
    # Mapa de stocks locales para actualizaciones rápidas
    existing_local_stocks = {s.id: s for s in tenant_db.query(Bsale_Stock).all()}
    
    # --- PASO NUEVO: Obtener sets de IDs válidos para las claves foráneas ---
    valid_variant_ids = {v.id for v in tenant_db.query(Bsale_Variant.id).all()}
    valid_office_ids = {o.id for o in tenant_db.query(Bsale_Office.id).all()}
    print(f"{len(existing_local_stocks)} stocks, {len(valid_variant_ids)} variantes, y {len(valid_office_ids)} oficinas locales cargadas.")

    # 3. Iterar y sincronizar
    new_stocks = 0
    updated_stocks = 0
    skipped_count = 0

    for api_stock in all_stocks_from_api:
        stock_id = api_stock.get('id')
        variant_id = api_stock.get('variant', {}).get('id')
        office_id = api_stock.get('office', {}).get('id')

        if not all([stock_id, variant_id, office_id]):
            skipped_count += 1
            continue

        local_stock = existing_local_stocks.get(stock_id)

        quantity = Decimal(api_stock.get('quantity', 0))
        quantity_reserved = Decimal(api_stock.get('quantityReserved', 0))
        quantity_available = Decimal(api_stock.get('quantityAvailable', 0))

        if local_stock:
            # Actualizar registro existente (lógica sin cambios)
            # ...
            pass # Placeholder para tu lógica de actualización existente
        else:
            # --- VALIDACIÓN AÑADIDA ANTES DE CREAR ---
            if variant_id in valid_variant_ids and office_id in valid_office_ids:
                # Si ambas claves foráneas son válidas, crear el nuevo registro
                new_stock_record = Bsale_Stock(
                    id=stock_id,
                    quantity=quantity,
                    quantity_reserved=quantity_reserved,
                    quantity_available=quantity_available,
                    variant_id=variant_id,
                    office_id=office_id
                )
                tenant_db.add(new_stock_record)
                new_stocks += 1
            else:
                # Si alguna clave foránea no existe, loguear y saltar
                print(f"ADVERTENCIA: Saltando registro de stock ID {stock_id} porque la variante ID {variant_id} o la oficina ID {office_id} no existen en la base de datos local.")
                skipped_count += 1
    
    if new_stocks > 0 or updated_stocks > 0:
        print(f"Guardando cambios en stock. Nuevos: {new_stocks}, Actualizados: {updated_stocks}.")
        tenant_db.commit()
    else:
        print("No se detectaron cambios en los niveles de stock.")
    
    if skipped_count > 0:
        print(f"Se saltaron {skipped_count} registros de stock por datos incompletos o claves foráneas inválidas.")

def _create_bsale_metric_alerts(tenant_db: Session, product_map: Dict, warehouse_map: Dict, api_key: str):
    """
    Calcula todas las métricas usando datos de Bsale para el día actual y guarda 
    los resultados en la tabla MetricAlert.
    """
    print("Calculando métricas y generando alertas desde datos de Bsale...")
    sync_date = date.today()
    
    # 1. Limpiar alertas existentes para hoy para esta sucursal/es.
    print(f"Limpiando alertas existentes para la fecha {sync_date}...")
    warehouse_ids = list(warehouse_map.values())
    tenant_db.query(MetricAlert).filter(
        MetricAlert.alert_date == sync_date,
        MetricAlert.warehouse_id.in_(warehouse_ids)
    ).delete(synchronize_session=False)
    tenant_db.commit()

    # 2. Pre-calcular datos necesarios para evitar queries en bucles
    # Cargar todas las configuraciones de métricas para todas las sucursales
    all_metric_names = list(MetricName)
    configs = {
        wh_id: {
            metric_name: get_effective_metric_config(tenant_db, metric_name, wh_id)
            for metric_name in all_metric_names
        } for wh_id in warehouse_ids
    }
    
    SALE_DOCUMENT_NAMES = ["BOLETA ELECTRÓNICA T",
                            "NOTA VENTA",
                            "FACTURA ELECTRÓNICA T",
                            "COMPROBANTE DE VENTA",
                            "BOLETA ELECTRONICA T",
                            "FACTURA ELECTRONICA T"
                            ]
    
    sale_doc_type_ids_tuples = tenant_db.query(Bsale_Document_Type.id)\
        .filter(Bsale_Document_Type.name.in_(SALE_DOCUMENT_NAMES)).all()
    
    SALE_DOCUMENT_TYPES = [res[0] for res in sale_doc_type_ids_tuples]
    print(f"IDs de documentos de Venta encontrados: {SALE_DOCUMENT_TYPES}")

    # 3. Iterar sobre todos los registros de stock de Bsale
    all_stock_records = tenant_db.query(Bsale_Stock, Bsale_Variant, Bsale_Office)\
        .join(Bsale_Variant).join(Bsale_Office).all()
    
    alerts_to_create = []

    for stock, variant, office in all_stock_records:
        product_code = variant.code or variant.bar_code
        if not product_code: continue

        product_info = product_map.get(product_code)
        warehouse_id = warehouse_map.get(office.name)
        if not product_info or not warehouse_id: continue

        # Obtener la configuración correcta para esta sucursal
        wh_configs = configs.get(warehouse_id, {})
        
        available_stock = Decimal(stock.quantity_available or 0)
        physical_stock = Decimal(stock.quantity or 0)

        # --- EVALUAR CADA MÉTRICA PARA ESTE PRODUCTO ---
        
        # a) Stock Cero
        if available_stock <= 0:
            # Consultar cuántas unidades se vendieron hoy para esta variante en esta sucursal
            quantity_sold_without_stock = tenant_db.query(
                func.sum(Bsale_Document_Detail.quantity)
            ).join(
                Bsale_Document, Bsale_Document_Detail.document_id == Bsale_Document.id
            ).filter(
                Bsale_Document_Detail.variant_id == variant.id, # Producto específico
                Bsale_Document.office_id == office.id,         # Sucursal específica
                func.date(Bsale_Document.date) == sync_date,   # Ventas del día de hoy
                Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES) # Solo documentos de venta
            ).scalar() or 0 # .scalar() para obtener un solo valor, o 0 si es None
            
            # 3. Crear la alerta con el valor calculado
            alerts_to_create.append({
                "metric_name": MetricName.STOCK_CERO,
                "code": product_code,
                "wh_id": office.id,
                "value": float(quantity_sold_without_stock)
            })

        # b) Stock Crítico y Sobre Stock (necesitan promedio de ventas)
        cfg_critico = wh_configs.get(MetricName.STOCK_CRITICO)
        if cfg_critico:
            avg_sales_start_dt = sync_date - timedelta(days=cfg_critico["days_for_avg"])
            total_sold = tenant_db.query(func.sum(Bsale_Document_Detail.quantity)).join(Bsale_Document)\
                .filter(
                    Bsale_Document_Detail.variant_id == variant.id,
                    Bsale_Document.office_id == office.id,
                    Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
                    cast(Bsale_Document.date, Date).between(avg_sales_start_dt, sync_date)
                ).scalar() or 0
            avg_daily_sales = Decimal(total_sold) / Decimal(cfg_critico["days_for_avg"])

            # Stock Crítico
            if (available_stock <= (avg_daily_sales * Decimal(cfg_critico["coverage_days_threshold"]))) or \
               (available_stock <= Decimal(cfg_critico["stock_qty_threshold"])):
                alerts_to_create.append({"metric_name": MetricName.STOCK_CRITICO, "value": available_stock, "code": product_code, "wh_id": office.id})

            # Sobre Stock
            cfg_sobrestock = wh_configs.get(MetricName.SOBRE_STOCK)
            if cfg_sobrestock and physical_stock > (avg_daily_sales * Decimal(cfg_sobrestock["coverage_days_threshold"])):
                alerts_to_create.append({"metric_name": MetricName.SOBRE_STOCK, "value": available_stock, "code": product_code, "wh_id": office.id})

        # c) Baja Rotación (necesita última fecha de venta)
        cfg_baja_rot = wh_configs.get(MetricName.BAJA_ROTACION)
        if cfg_baja_rot and physical_stock > 0:
            last_sale = tenant_db.query(func.max(cast(Bsale_Document.date, Date))).join(Bsale_Document_Detail)\
                .filter(
                    Bsale_Document_Detail.variant_id == variant.id,
                    Bsale_Document.office_id == office.id,
                    Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES)
                ).scalar()
            
            days_no_sale = (sync_date - last_sale).days if last_sale else 999
            if days_no_sale > cfg_baja_rot["days_since_last_sale"]:
                alerts_to_create.append({"metric_name": MetricName.BAJA_ROTACION, "value": available_stock, "code": product_code, "wh_id": office.id})
    
        # --- d) RECOMENDACIÓN DE COMPRA ---
        cfg_recompra = wh_configs.get(MetricName.RECOMENDACION_COMPRA)
        if cfg_recompra:
            recompra_start_dt = sync_date - timedelta(days=cfg_recompra["sales_days_for_recommendation"])
            
            total_sold_lookback = tenant_db.query(func.sum(Bsale_Document_Detail.quantity))\
                .join(Bsale_Document)\
                .filter(
                    Bsale_Document_Detail.variant_id == variant.id,
                    Bsale_Document.office_id == office.id,
                    Bsale_Document.document_type_id.in_(SALE_DOCUMENT_TYPES),
                    cast(Bsale_Document.date, Date).between(recompra_start_dt, sync_date)
                ).scalar() or 0
            
            if available_stock < total_sold_lookback:

                units_to_buy = total_sold_lookback - available_stock
                alerts_to_create.append({
                    "metric_name": MetricName.RECOMENDACION_COMPRA, 
                    "value": units_to_buy, 
                    "code": product_code, 
                    "wh_id": office.id
                })

    # --- AJUSTES DE STOCK ---
    print("Procesando Ajustes de Stock desde Consumos de Bsale...")
    consumptions_today = get_stock_consumptions_for_date(api_key, sync_date)
    
    # Agregamos las cantidades por producto para crear una sola alerta por producto
    adjustments_by_product = defaultdict(lambda: {'quantity': 0, 'office_id': None})

    for consumption in consumptions_today:
        office_id_str = consumption.get("office", {}).get("id")
        if not office_id_str: continue

        for detail in consumption.get("details", {}).get("items", []):
            variant_id = detail.get("variant", {}).get("id")
            if not variant_id: continue

            # La cantidad en "consumptions" puede ser positiva (ajuste de entrada) o
            # negativa (ajuste de salida). Aquí sumamos el valor absoluto.
            quantity_adjusted = abs(Decimal(detail.get("quantity", 0)))

            # Usamos el ID de la variante para agrupar
            key = int(variant_id)
            adjustments_by_product[key]['quantity'] += quantity_adjusted
            adjustments_by_product[key]['office_id'] = int(office_id_str)
    
    # Convertir los ajustes agregados a formato de alerta
    # Necesitamos un mapa de variant_id -> product_code
    variant_id_map = {v.id: (v.code or v.bar_code) for v in tenant_db.query(Bsale_Variant).all()}

    for variant_id, data in adjustments_by_product.items():
        product_code = variant_id_map.get(variant_id)
        if product_code:
            alerts_to_create.append({
                "metric_name": MetricName.AJUSTE_STOCK,
                "code": product_code,
                "wh_id": data['office_id'],
                "value": data['quantity']
            })  
    
        # 1. Procesar Devoluciones
    
    print("Procesando Devoluciones desde la API de Bsale...")
    returns_today = get_returns_for_date(api_key, sync_date)
    
    # Pre-mapear los documentDetailId a variant_id para eficiencia
    all_doc_detail_ids = [
        detail.get("documentDetailId") 
        for ret in returns_today 
        for detail in ret.get("details", {}).get("items", [])
        if detail.get("documentDetailId")
    ]
    
    doc_detail_to_variant_map = {
        detail.id: detail.variant_id 
        for detail in tenant_db.query(Bsale_Document_Detail).filter(Bsale_Document_Detail.id.in_(all_doc_detail_ids)).all()
    }
    
    # Pre-mapear variant_id a product_code
    variant_id_map = {v.id: (v.code or v.bar_code) for v in tenant_db.query(Bsale_Variant).all()}

    # Agregar las devoluciones por producto y sucursal
    returns_by_product_wh = defaultdict(lambda: 0)
    for ret in returns_today:
        office_id = int(ret.get("office", {}).get("id", 0))
        if not office_id: continue

        for detail in ret.get("details", {}).get("items", []):
            doc_detail_id = detail.get("documentDetailId")
            variant_id = doc_detail_to_variant_map.get(doc_detail_id)
            if not variant_id: continue

            product_code = variant_id_map.get(variant_id)
            if not product_code: continue
            
            quantity_returned = Decimal(detail.get("quantity", 0))
            returns_by_product_wh[(product_code, office_id)] += quantity_returned

    # Convertir los datos agregados al formato de alerta
    for (product_code, wh_id), total_quantity in returns_by_product_wh.items():
        alerts_to_create.append({
            "metric_name": MetricName.DEVOLUCIONES,
            "code": product_code,
            "wh_id": wh_id,
            "value": total_quantity
        })
      
   # 5. Agrupar todas las alertas recolectadas por tipo de métrica
    grouped_alerts = defaultdict(list)
    for alert_item in alerts_to_create:
        grouped_alerts[alert_item['metric_name']].append(alert_item)

    # 6. USAR EL HELPER: Iterar sobre los grupos y llamar al helper de inserción
    current_stock_by_code = { (v.code or v.bar_code): s.quantity_available for s,v,o in all_stock_records }
    for metric_name, alert_list in grouped_alerts.items():
        _create_metric_alerts(
            tenant_db=tenant_db,
            alert_data_list=alert_list,
            product_map=product_map,
            stock_map=current_stock_by_code,
            metric_name=metric_name,
            sync_date=sync_date
        )

    tenant_db.commit()
    print("Alertas de métricas de Bsale guardadas.")

def run_full_bsale_etl(company_id: int):
    """
    Función completa que se ejecuta en segundo plano. Es autosuficiente.
    """
    print(f"--- INICIANDO ETL COMPLETO DE BSALE PARA COMPAÑÍA ID: {company_id} ---")
    
    # Una tarea de fondo debe crear y gestionar su propia sesión de DB
    main_db: Session = SessionLocal()
    tenant_session: Optional[Session] = None
    
    try:
        # 1. Obtener la compañía y su API Key desde la DB principal
        company = main_db.query(Company).filter_by(id=company_id).first()
        if not company:
            raise Exception(f"Compañía con ID {company_id} no encontrada.")
        
        if not company.api_key:
            raise Exception(f"Compañía '{company.name}' no tiene una API Key de Bsale configurada.")

        # 2. Conectar a la base de datos del inquilino
        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_session = get_tenant_session(tenant_db_url)
        if not tenant_session:
            raise Exception("No se pudo establecer conexión con la base de datos del inquilino.")

        # Paso A: Sincronizar Oficinas -> Sucursales
        warehouse_map = _sync_offices_to_warehouses(tenant_session)

        # Paso B: Sincronizar Variantes -> Productos
        product_map = _sync_variants_to_products(tenant_session)

        #sync_bsale_stock_levels(tenant_session, company.api_key)

        # Paso C: Calcular y guardar las alertas, pasando la api_key necesaria
        _create_bsale_metric_alerts(tenant_db=tenant_session,api_key=company.api_key,product_map=product_map,warehouse_map=warehouse_map)
        print("--- ETL DE BSALE FINALIZADO EXITOSAMENTE ---")
        # Aquí podrías enviar una notificación de éxito
        
    except Exception as e:
        print(f"--- ERROR DURANTE EL ETL DE BSALE ---")
        traceback.print_exc()
        # Aquí podrías enviar una notificación de error
    finally:
        # Es crucial cerrar ambas sesiones
        if tenant_session: tenant_session.close()
        if main_db: main_db.close()