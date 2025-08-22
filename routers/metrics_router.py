import uuid
import traceback
from fastapi import APIRouter, Depends, HTTPException, Query, status, Response
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import date, timedelta
from typing import List, Optional, Dict, Tuple
from decimal import Decimal
from fastapi_cache.decorator import cache
# --- Dependencias y Modelos ---
from database.main_db import get_db as get_main_db
from database.dynamic import get_tenant_session
from models.main_db import Company, User as UserModel, MetricDisplayConfiguration
from models.tenant_db import DailyStockSnapshot, GeneratedMetricReport, Product, Warehouse, MetricAlert, MetricName
from schemas.metric_schemas import (
    MetricsApiResponse, MetricGridItem, MetricDataItem, MetricsApiRequest,
    OverviewResponse, OverviewDataPoint,
    CounterItem, DistributionItem, EscalationItem, EscalationProductItem
)
from auth.auth_bearer import get_current_active_user
from services.jl_caspana_service import get_rp_sistemas_db_session # Para el caso especial

# --- Creación del Router ---
router = APIRouter(
    prefix="/metrics",
    tags=["Metrics Reporting"]
)

# --- Helpers ---
def get_consecutive_alert_days(
    db: Session,
    product_id: int,
    warehouse_id: int,
    metric_name: MetricName,
    end_date: date,
    max_lookback_days: int = 30
) -> int:
    """
    Calcula cuántos días consecutivos, terminando en end_date,
    un producto/sucursal ha estado en un estado de alerta específico.
    """
    consecutive_days = 0
    for i in range(max_lookback_days + 1):
        current_check_date = end_date - timedelta(days=i)
        
        # Como me indicaste, tu configuración usa el objeto Enum directamente en la query.
        alert_exists = db.query(MetricAlert.id).filter(
            MetricAlert.alert_date == current_check_date,
            MetricAlert.product_id == product_id,
            MetricAlert.warehouse_id == warehouse_id,
            MetricAlert.metric_name == metric_name
        ).first()
        
        if alert_exists:
            consecutive_days += 1
        else:
            # En cuanto encontramos un día sin alerta, la racha se rompe.
            break
            
    return consecutive_days

# Plantilla de respaldo si una métrica no tiene configuración de visualización en la DB.
DEFAULT_DISPLAY_CONFIG = {
    "title": "Reporte de Métrica", "name": "Métrica sin Nombre", "abbreviation": "N/A",
    "information_1": "Información no disponible.",
    "product_operation": "greater", "product_number": 3,
    "background_color_1": "#F5F5F5", "text_color_1": "#212121", "text_color_2": "#757575",
    "background_color_3": "#E0E0E0", "text_color_3": "#212121",
    "background_color_4": "#9E9E9E", "text_color_4": "#FFFFFF",
    "show_counter": True, "show_grid": True, "show_distribution": True, "show_products": True,
}

# --- Endpoint Principal ---
@router.get("/{company_id}", response_model=MetricsApiResponse)

async def get_company_metrics_report(
    company_id: int,
    report_params: MetricsApiRequest = Depends(),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Obtiene un reporte de métricas pre-calculado. Si el rango de fechas
    es mayor a un día, agrega los datos de los reportes diarios.
    Si no se especifica sucursal, agrega los datos de todas.
    """
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized for this company's metrics")

    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="Database configuration for company is incomplete.")
    
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        
        target_warehouse_id: Optional[int] = None
        if report_params.store:
            warehouse_obj = tenant_session.query(Warehouse.id).filter(func.lower(Warehouse.name) == report_params.store.lower()).first()
            if not warehouse_obj:
                raise HTTPException(status_code=404, detail=f"Sucursal '{report_params.store}' no encontrada.")
            target_warehouse_id = warehouse_obj[0]

        # 2. Leer reportes pre-calculados de la nueva tabla
        query = tenant_session.query(GeneratedMetricReport.report_date, GeneratedMetricReport.report_data).filter(
            GeneratedMetricReport.report_date.between(report_params.dateInit, report_params.dateEnd)
        )
        # Si se especifica una tienda, se filtra. Si no, se obtienen todas.
        if target_warehouse_id:
            query = query.filter(GeneratedMetricReport.warehouse_id == target_warehouse_id)

        daily_reports_raw = query.order_by(GeneratedMetricReport.report_date).all()
        daily_reports_data = [report[0] for report in query.all()]

        if not daily_reports_data:
            print(f"No se encontraron reportes. Generando respuesta por defecto para la compañía {company_id}.")
            
            # Cargar las configuraciones de visualización para construir la respuesta
            display_configs_from_db = main_db.query(MetricDisplayConfiguration).filter_by(company_id=company_id).all()
            display_configs_map = {config.metric_id: config for config in display_configs_from_db}

            default_grid = []
            default_counters = []
            default_products = []

            for metric_enum in MetricName:
                config = display_configs_map.get(metric_enum)
                if not config: continue
                default_grid.append(MetricGridItem(name=config.name, metricId=metric_enum, data=[]))
                default_counters.append(CounterItem(
                    name=config.name,
                    quantity=0,
                    information=config.information_1 or "",
                    backgroundColor=config.background_color_1 or "#FFFFFF",
                    text1Color=config.text_color_1 or "#000000",
                    text2Color=config.text_color_2 or "#000000",
                    amount=0,
                    price=0.0
                ))

                default_products.append(EscalationItem(
                    title=config.title, name=config.name, abbr=config.abbreviation or "",
                    operation=config.product_operation, value=config.product_number,
                    quantity=0, information=config.information_1 or "",
                    background3Color=config.background_color_3 or "#FFFFFF",
                    text3Color=config.text_color_3 or "#000000",
                    background4Color=config.background_color_4 or "#FFFFFF",
                    text4Color=config.text_color_4 or "#000000",
                    amountAlert=0, products=[]
                ))

            return MetricsApiResponse(
                grid=default_grid,
                overview=OverviewResponse(
                    data=[OverviewDataPoint(date=report_params.dateEnd, net=0, deviation=0)]
                ),
                counters=default_counters,
                distributions=[DistributionItem(name=c.name, price=0.0) for c in default_counters],
                products=default_products
            )

        # 3. Agregar los datos de los reportes diarios
        aggregated_grid: Dict[str, MetricGridItem] = {}
        aggregated_counters: Dict[str, CounterItem] = {}
        aggregated_overview_points: List[OverviewDataPoint] = []
        aggregated_escalation_items: Dict[str, EscalationItem] = {}

        for report_date_obj, report_data in daily_reports_raw:
            # Agregar Overview
            if report_data.get("overview") and report_data["overview"].get("data"):
                aggregated_overview_points.extend([OverviewDataPoint(**dp) for dp in report_data["overview"]["data"]])
            
            # Agregar Grid
            for grid_item_data in report_data.get("grid", []):
                metric_id = grid_item_data["metricId"]
                if metric_id not in aggregated_grid:
                    aggregated_grid[metric_id] = MetricGridItem(**grid_item_data)
                else:
                    aggregated_grid[metric_id].data.extend([MetricDataItem(**di) for di in grid_item_data["data"]])

            # Agregar Counters
            for counter_item_data in report_data.get("counters", []):
                name = counter_item_data["name"]
                if name not in aggregated_counters:
                    aggregated_counters[name] = CounterItem(**counter_item_data)
                else:
                    aggregated_counters[name].quantity += counter_item_data.get("quantity", 0)
                    aggregated_counters[name].amount = (aggregated_counters[name].amount or 0) + (counter_item_data.get("amount") or 0)
                    aggregated_counters[name].price = (aggregated_counters[name].price or 0.0) + (counter_item_data.get("price") or 0.0)

            # Agregar productos/escalamiento solo para el último día del rango
            for escalation_item_data in report_data.get("products", []):
                name = escalation_item_data["name"]
                if name not in aggregated_escalation_items:
                    aggregated_escalation_items[name] = EscalationItem(**escalation_item_data)
                else:
                    aggregated_escalation_items[name].quantity += escalation_item_data.get("quantity", 0)
                    aggregated_escalation_items[name].amountAlert += escalation_item_data.get("amountAlert", 0)
                    new_products = [EscalationProductItem(**p) for p in escalation_item_data.get("products", [])]
                    aggregated_escalation_items[name].products.extend(new_products)
        
        print(len(aggregated_escalation_items), "escalation items agregados.")
        # Post-procesamiento para el escalamiento agregado
        for item in aggregated_escalation_items.values():
            item.products.sort(key=lambda p: p.alertDays, reverse=True)
            item.products = item.products[:4] 

        # 4. Construir la respuesta final
        final_counters = list(aggregated_counters.values())
        final_distributions = [DistributionItem(name=c.name, price=c.price or 0.0) for c in final_counters]
        
        print(f"Reportes agregados: {len(aggregated_grid)} métricas, {len(final_counters)} contadores, {len(aggregated_escalation_items)} productos escalados.")

        return MetricsApiResponse(
            grid=list(aggregated_grid.values()),
            overview=OverviewResponse(data=aggregated_overview_points),
            counters=final_counters,
            distributions=final_distributions,
            products=list(aggregated_escalation_items.values())
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno del servidor al leer reporte: {str(e)}")
    finally:
        if tenant_session:
            tenant_session.close()

async def _create_report_for_warehouse(
    tenant_session: Session,
    main_db: Session,
    company_id: int,
    target_date: date,
    target_warehouse_id: int,
    display_configs_map: dict,
    rp_sistemas_session: Optional[Session]
):
    """Función helper que contiene la lógica para generar y guardar un reporte para una única sucursal."""
    # --- 1. CÁLCULO PARA "OVERVIEW" ---
    overview_data_points: List[OverviewDataPoint] = []
    net_value_for_caspana = Decimal(0)
    if company_id == 47 and rp_sistemas_session:
        query_total_value = text("SELECT SUM(A.CANT_STOCK * A.PRECIO_UNI) FROM ARTICULOS A WHERE A.ACTIVO = 'S' AND A.COD_ARTICULO NOT IN ('SI', 'VC', 'VD') AND A.USA_COMPO <> 'S'")
        net_value_for_caspana = Decimal(rp_sistemas_session.execute(query_total_value).scalar() or 0)

    net_val_day = net_value_for_caspana if company_id == 47 else (
        tenant_session.query(func.sum(DailyStockSnapshot.closing_physical_stock * Product.cost))
        .join(Product, DailyStockSnapshot.product_id == Product.id)
        .filter(DailyStockSnapshot.snapshot_date == target_date, DailyStockSnapshot.warehouse_id == target_warehouse_id)
        .scalar() or Decimal(0)
    )
    q_alert = tenant_session.query(func.sum(MetricAlert.physical_stock_at_alert * Product.cost))\
        .join(Product, MetricAlert.product_id == Product.id)\
        .filter(MetricAlert.alert_date == target_date, MetricAlert.metric_name == MetricName.STOCK_CRITICO, MetricAlert.warehouse_id == target_warehouse_id)
    alert_val_day = q_alert.scalar() or Decimal(0)
    overview_data_points.append(OverviewDataPoint(date=target_date, net=float(net_val_day), deviation=float(alert_val_day)))
    overview_response = OverviewResponse(data=overview_data_points)

    # --- 2. CÁLCULO PARA "GRID", "COUNTERS", "DISTRIBUTIONS", "PRODUCTS" ---
    metric_results_grid: List[MetricGridItem] = []
    all_counters: List[CounterItem] = []
    all_escalation_items: List[EscalationItem] = []
    
    for metric_enum in list(MetricName):
        display_config = display_configs_map.get(metric_enum)
        if not display_config:
            continue

        q_alerts = tenant_session.query(MetricAlert, Product, Warehouse)\
            .join(Product, MetricAlert.product_id == Product.id)\
            .join(Warehouse, MetricAlert.warehouse_id == Warehouse.id)\
            .filter(MetricAlert.alert_date == target_date, MetricAlert.metric_name == metric_enum, MetricAlert.warehouse_id == target_warehouse_id)
        
        all_alerts_for_metric = q_alerts.all()
        
        current_metric_data_items: List[MetricDataItem] = []
        for alert, prod, wh in all_alerts_for_metric:
            alert_days_val = alert.details_json.get("consecutive_days") if alert.details_json else None
            item_data = MetricDataItem(
                product_sku=prod.code, product_name=prod.name, store_name=wh.name, category=prod.category,
                physical_stock=alert.physical_stock_at_alert, reserved_stock=alert.reserved_stock_at_alert,
                available_stock=alert.available_stock_at_alert,
                metric_value=alert.metric_value_numeric if alert.metric_value_numeric is not None else alert.metric_value_text,
                alert_days=alert_days_val, alert_level=alert.details_json.get("alertLevel", 1) if alert.details_json else 1,
                cost=float(prod.cost or 0), snapshot_date=alert.alert_date
            )
            current_metric_data_items.append(item_data)
        
        if display_config.show_grid:
            metric_results_grid.append(MetricGridItem(name=display_config.name, metricId=metric_enum, data=current_metric_data_items))

        if display_config.show_counter:
            total_units = 0; total_value = Decimal(0)
            unique_product_costs = {item.product_sku: Decimal(item.cost or 0) for item in current_metric_data_items}
            if metric_enum == MetricName.STOCK_CERO:
                total_units = 0; total_value = sum(unique_product_costs.values())
            else:
                for item in current_metric_data_items:
                    cost = Decimal(item.cost or 0); units_affected = 0
                    if metric_enum == MetricName.STOCK_CRITICO: units_affected = item.available_stock or 0
                    elif metric_enum in [MetricName.BAJA_ROTACION, MetricName.SOBRE_STOCK]: units_affected = item.physical_stock or 0
                    elif metric_enum == MetricName.VENTA_SIN_STOCK: units_affected = abs(item.physical_stock or 0)
                    elif metric_enum in [MetricName.DEVOLUCIONES, MetricName.AJUSTE_STOCK, MetricName.RECOMENDACION_COMPRA]: units_affected = int(Decimal(item.metric_value or 0))
                    total_units += units_affected; total_value += Decimal(units_affected) * cost
            all_counters.append(CounterItem(
                name=display_config.name, quantity=len(unique_product_costs),
                information=display_config.information_1 or "", backgroundColor=display_config.background_color_1 or "#F5F5F5",
                text1Color=display_config.text_color_1 or "#212121", text2Color=display_config.text_color_2 or "#757575",
                amount=total_units, price=float(total_value)
            ))

        if display_config.show_products:
            escalated_products = []
            for alert, product, warehouse in all_alerts_for_metric:
                consecutive_days = get_consecutive_alert_days(tenant_session, alert.product_id, alert.warehouse_id, alert.metric_name, target_date)
                if consecutive_days > (display_config.product_number or 3):
                    escalated_products.append(EscalationProductItem(name=product.name, alertDays=consecutive_days))
            escalated_products.sort(key=lambda x: x.alertDays, reverse=True)
            counter_for_metric = next((c for c in all_counters if c.name == display_config.name), None)
            all_escalation_items.append(EscalationItem(
                title=display_config.title or "", name=display_config.name, abbr=display_config.abbreviation or "N/A",
                operation=display_config.product_operation or "greater", value=display_config.product_number or 3,
                quantity=counter_for_metric.quantity if counter_for_metric else 0, information=display_config.information_1 or "",
                background3Color=display_config.background_color_3 or "#E0E0E0", text3Color=display_config.text_color_3 or "#212121",
                background4Color=display_config.background_color_4 or "#9E9E9E", text4Color=display_config.text_color_4 or "#FFFFFF",
                amountAlert=len(escalated_products), products=escalated_products[:4]
            ))
    
    final_response_object = MetricsApiResponse(
        grid=metric_results_grid, overview=overview_response, counters=all_counters,
        distributions=[DistributionItem(name=c.name, price=c.price or 0.0) for c in all_counters],
        products=all_escalation_items
    )
    
    report_data_dict = jsonable_encoder(final_response_object)
    existing_report = tenant_session.query(GeneratedMetricReport).filter(
        GeneratedMetricReport.report_date == target_date,
        GeneratedMetricReport.warehouse_id == target_warehouse_id
    ).first()

    if existing_report:
        existing_report.report_data = report_data_dict
        existing_report.updated_at = func.now()
    else:
        new_report = GeneratedMetricReport(report_date=target_date, warehouse_id=target_warehouse_id, report_data=report_data_dict)
        tenant_session.add(new_report)
    
    tenant_session.commit()

# --- Endpoint de Generación (Nuevo) ---
@router.post("/generate-daily-report/{company_id}", status_code=status.HTTP_201_CREATED)
async def generate_daily_report(
    company_id: int,
    report_date: Optional[date] = Query(None, description="Fecha para la cual generar el reporte. Por defecto, hoy."),
    store_name: Optional[str] = Query(None, description="Nombre de la sucursal para generar un reporte específico. Si se omite, se generan para todas."),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Calcula y guarda el reporte de métricas para un día.
    Si no se especifica 'store_name', itera y genera un reporte para CADA sucursal.
    """
    target_date = report_date or date.today()
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company: raise HTTPException(status_code=404, detail="Company not found")
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated: raise HTTPException(status_code=403, detail="Not authorized for this company's metrics")
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]): raise HTTPException(status_code=500, detail="Database configuration for company is incomplete.")
    
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None
    rp_sistemas_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        display_configs_from_db = main_db.query(MetricDisplayConfiguration).filter_by(company_id=company_id).all()
        display_configs_map = {config.metric_id: config for config in display_configs_from_db}

        if company_id == 47:
            rp_sistemas_session = get_rp_sistemas_db_session(company_id, main_db)
            if not rp_sistemas_session: raise HTTPException(status_code=503, detail="No se pudo conectar a la base de datos de RP Sistemas.")

        warehouses_to_process: List[Warehouse] = []
        if store_name:
            warehouse_obj = tenant_session.query(Warehouse).filter(func.lower(Warehouse.name) == store_name.lower()).first()
            if not warehouse_obj: raise HTTPException(status_code=404, detail=f"Sucursal '{store_name}' no encontrada.")
            warehouses_to_process.append(warehouse_obj)
        else:
            warehouses_to_process = tenant_session.query(Warehouse).all()
            if not warehouses_to_process: raise HTTPException(status_code=404, detail="No se encontraron sucursales para esta compañía.")

        for warehouse in warehouses_to_process:
            await _create_report_for_warehouse(
                tenant_session=tenant_session, main_db=main_db, company_id=company_id,
                target_date=target_date, target_warehouse_id=warehouse.id,
                display_configs_map=display_configs_map, rp_sistemas_session=rp_sistemas_session
            )
        
        return {"message": f"Proceso completado. Se generaron/actualizaron reportes para {len(warehouses_to_process)} sucursal(es)."}

    except HTTPException as http_exc:
        if tenant_session: tenant_session.rollback()
        raise http_exc
    except Exception as e:
        traceback.print_exc()
        if tenant_session: tenant_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error interno del servidor al generar reporte: {str(e)}")
    finally:
        if tenant_session: tenant_session.close()
        if rp_sistemas_session: rp_sistemas_session.close()