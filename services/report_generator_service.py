import uuid
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, Dict, Tuple, List
from datetime import date, timedelta
import traceback

from models.tenant_db import GeneratedReport, MetricAlert, Product, Warehouse, MetricName
from database.dynamic import get_tenant_session
from schemas.report_schemas import ReportOut, ProductAlertOut, OccurrencesAlertOut
from utils.calculate_alerts_days import get_consecutive_alert_days

def generate_and_save_daily_report(db_url: str, for_date: date, company_id: int, warehouse_id: Optional[int] = None):
    """
    Función de fondo que calcula el reporte para un día, lo guarda en la DB,
    y calcula correctamente los días de alerta consecutivos.
    """
    tenant_session: Optional[Session] = None
    print(f"Iniciando generación de reporte para la fecha: {for_date}...")
    
    try:
        tenant_session = get_tenant_session(db_url)
        
        query = tenant_session.query(MetricAlert, Product, Warehouse)\
            .join(Product, MetricAlert.product_id == Product.id)\
            .join(Warehouse, MetricAlert.warehouse_id == Warehouse.id)\
            .filter(MetricAlert.alert_date == for_date)
        
        if warehouse_id:
            query = query.filter(MetricAlert.warehouse_id == warehouse_id)
        
        all_alerts_for_day = query.order_by(Product.name, Warehouse.name).all()
        print(f"Se encontraron {len(all_alerts_for_day)} ocurrencias de alertas para procesar.")

        product_alerts_map: Dict[Tuple[int, int], ProductAlertOut] = {}
        for alert, product, warehouse in all_alerts_for_day:
            product_key = (product.id, warehouse.id)
            
            if product_key not in product_alerts_map:
                product_alerts_map[product_key] = ProductAlertOut(
                    id=f"prod_{product.id}-wh_{warehouse.id}",
                    name=product.name,
                    product_sku=product.code,
                    store=warehouse.name,
                    occurrences=[]
                )
            
            consecutive_alert_days = get_consecutive_alert_days(
                db=tenant_session,
                product_id=product.id,
                warehouse_id=warehouse.id,
                metric_name=alert.metric_name,
                end_date=alert.alert_date
            )
            occurrence = OccurrencesAlertOut(
                id=str(alert.id),
                metric_id=alert.metric_name,
                date=alert.alert_date,
                alert_days=consecutive_alert_days
            )
            
            product_alerts_map[product_key].occurrences.append(occurrence)

        report_to_save = ReportOut(
            id=str(uuid.uuid4()),
            init_date=for_date,
            end_date=for_date,
            products=list(product_alerts_map.values())
        )

        new_db_report = GeneratedReport(
            id=report_to_save.id,
            report_date=for_date,
            warehouse_id=warehouse_id,
            report_data=report_to_save.model_dump(mode='json', by_alias=True)
        )
        tenant_session.add(new_db_report)
        tenant_session.commit()
        print(f"Reporte con ID {new_db_report.id} guardado exitosamente en la base de datos.")

    except Exception as e:
        print("--- ERROR DURANTE LA GENERACIÓN DEL REPORTE DIARIO ---")
        traceback.print_exc()
        if tenant_session: tenant_session.rollback()
    finally:
        if tenant_session: tenant_session.close()