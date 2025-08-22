import traceback
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import date, timedelta
from typing import List, Optional, Dict
from decimal import Decimal
from fastapi.encoders import jsonable_encoder

from models.main_db import Company, MetricDisplayConfiguration
from models.tenant_db import (
    DailyStockSnapshot, Product, Warehouse, MetricAlert, MetricName, GeneratedMetricReport
)
from schemas.metric_schemas import (
    MetricsApiResponse, MetricGridItem, MetricDataItem, OverviewResponse, 
    OverviewDataPoint, CounterItem, DistributionItem, EscalationItem, EscalationProductItem
)


def get_consecutive_alert_days(
    db: Session,
    product_id: int,
    warehouse_id: int,
    metric_name: MetricName,
    end_date: date,
    max_lookback_days: int = 30
) -> int:
    """Calcula los días de alerta consecutivos para un producto/sucursal."""
    alert_dates = {
        r[0] for r in db.query(MetricAlert.alert_date).filter(
            MetricAlert.product_id == product_id,
            MetricAlert.warehouse_id == warehouse_id,
            MetricAlert.metric_name == metric_name,
            MetricAlert.alert_date.between(end_date - timedelta(days=max_lookback_days), end_date)
        ).all()
    }
    if not alert_dates:
        return 0
    
    consecutive_days = 0
    for i in range(max_lookback_days + 1):
        current_check_date = end_date - timedelta(days=i)
        if current_check_date in alert_dates:
            consecutive_days += 1
        else:
            break
    return consecutive_days

async def generate_and_save_metric_reports(
    tenant_session: Session,
    main_db: Session,
    company_id: int,
    dates_to_process: List[date],
    warehouse_ids: List[int]
):
    """
    Genera y guarda los reportes de métricas para un conjunto de fechas y sucursales.
    Esta función es llamada tanto por la carga masiva como por la generación diaria.
    """
    print(f"Iniciando generación de reportes para compañía {company_id}...")
    print(f"Fechas a procesar: {dates_to_process}")
    print(f"Sucursales a procesar: {warehouse_ids}")

    try:
        # Cargar configuraciones de visualización una sola vez
        display_configs_from_db = main_db.query(MetricDisplayConfiguration).filter_by(company_id=company_id).all()
        display_configs_map = {config.metric_id: config for config in display_configs_from_db}

        # Iterar por cada sucursal y cada fecha
        for warehouse_id in warehouse_ids:
            for target_date in dates_to_process:
                # --- Lógica de cálculo para una fecha y sucursal específicas ---
                # (Esta es la misma lógica que ya tenías en _create_report_for_warehouse)
                
                # 1. Overview
                net_val_day = (
                    tenant_session.query(func.sum(DailyStockSnapshot.closing_physical_stock * Product.cost))
                    .join(Product, DailyStockSnapshot.product_id == Product.id)
                    .filter(DailyStockSnapshot.snapshot_date == target_date, DailyStockSnapshot.warehouse_id == warehouse_id)
                    .scalar() or Decimal(0)
                )
                alert_val_day = (
                    tenant_session.query(func.sum(MetricAlert.physical_stock_at_alert * Product.cost))
                    .join(Product, MetricAlert.product_id == Product.id)
                    .filter(MetricAlert.alert_date == target_date, MetricAlert.metric_name == MetricName.STOCK_CRITICO, MetricAlert.warehouse_id == warehouse_id)
                    .scalar() or Decimal(0)
                )
                overview_response = OverviewResponse(data=[OverviewDataPoint(date=target_date, net=float(net_val_day), deviation=float(alert_val_day))])

                # 2. Grid, Counters, Products, etc.
                metric_results_grid: List[MetricGridItem] = []
                all_counters: List[CounterItem] = []
                all_escalation_items: List[EscalationItem] = []

                for metric_enum in list(MetricName):
                    display_config = display_configs_map.get(metric_enum)
                    if not display_config: continue

                    q_alerts = tenant_session.query(MetricAlert, Product, Warehouse)\
                        .join(Product, MetricAlert.product_id == Product.id)\
                        .join(Warehouse, MetricAlert.warehouse_id == Warehouse.id)\
                        .filter(MetricAlert.alert_date == target_date, MetricAlert.metric_name == metric_enum, MetricAlert.warehouse_id == warehouse_id)
                    
        
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
                # Ensamblar y guardar el reporte
                final_response_object = MetricsApiResponse(
                    grid=metric_results_grid, overview=overview_response, counters=all_counters,
                    distributions=[DistributionItem(name=c.name, price=c.price or 0.0) for c in all_counters],
                    products=all_escalation_items
                )
                
                report_data_dict = jsonable_encoder(final_response_object)
                
                # Upsert (Update or Insert)
                existing_report = tenant_session.query(GeneratedMetricReport).filter(
                    GeneratedMetricReport.report_date == target_date,
                    GeneratedMetricReport.warehouse_id == warehouse_id
                ).first()

                if existing_report:
                    existing_report.report_data = report_data_dict
                    existing_report.updated_at = func.now()
                else:
                    new_report = GeneratedMetricReport(report_date=target_date, warehouse_id=warehouse_id, report_data=report_data_dict)
                    tenant_session.add(new_report)
        
        tenant_session.commit()
        print(f"Generación de reportes para compañía {company_id} completada.")

    except Exception as e:
        tenant_session.rollback()
        print(f"ERROR durante la generación de reportes para compañía {company_id}: {e}")
        traceback.print_exc()