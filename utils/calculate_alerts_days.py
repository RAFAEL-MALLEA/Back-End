from datetime import date, timedelta
from requests import Session
from models.tenant_db import MetricAlert, MetricName


def get_consecutive_alert_days(
    db: Session, 
    product_id: int, 
    warehouse_id: int, 
    metric_name: MetricName,
    end_date: date,
    max_lookback_days: int = 90
) -> int:
    """
    Calcula cuántos días consecutivos, terminando en `end_date`,
    un producto/sucursal ha estado en un estado de alerta específico.
    """
    consecutive_days = 0
    for i in range(max_lookback_days + 1):
        current_check_date = end_date - timedelta(days=i)
        
        alert_exists = db.query(MetricAlert.id).filter(
            MetricAlert.alert_date == current_check_date,
            MetricAlert.product_id == product_id,
            MetricAlert.warehouse_id == warehouse_id,
            MetricAlert.metric_name == metric_name
        ).first()
        
        if alert_exists:
            consecutive_days += 1
        else:
            break 
            
    return consecutive_days