from sqlalchemy.orm import Session
from models.tenant_db import (MetricConfiguration, MetricName)
from schemas.metric_schemas import MetricsApiRequest
from typing import Any, Dict, Optional, Set

def get_effective_metric_config( # Renombrada para mayor claridad
    tenant_db: Session,
    metric_name_enum: MetricName,
    warehouse_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Obtiene la configuración efectiva para una métrica, aplicando la jerarquía correcta.
    """
    # 1. Empezar con los valores por defecto codificados
    hardcoded_defaults = {}
    if metric_name_enum == MetricName.STOCK_CRITICO:
        hardcoded_defaults = {"days_for_avg": 30, "coverage_days_threshold": 3, "stock_qty_threshold": 3}
    elif metric_name_enum == MetricName.SOBRE_STOCK:
        hardcoded_defaults = {"days_for_avg": 30, "coverage_days_threshold": 30}
    elif metric_name_enum == MetricName.BAJA_ROTACION:
        hardcoded_defaults = {"days_since_last_sale": 14}
    elif metric_name_enum == MetricName.RECOMENDACION_COMPRA:
        hardcoded_defaults = {"sales_days_for_recommendation": 15}

    effective_config = hardcoded_defaults.copy()

    # 2. Cargar y aplicar configuración GLOBAL (sobrescribe defaults)
    global_db_conf_json = tenant_db.query(MetricConfiguration.config_json).filter(
        MetricConfiguration.metric_name == metric_name_enum,
        MetricConfiguration.warehouse_id == None,
        MetricConfiguration.is_active == True
    ).scalar()
    
    if global_db_conf_json:
        effective_config.update(global_db_conf_json)

    # 3. Cargar y aplicar configuración ESPECÍFICA de sucursal (sobrescribe global y defaults)
    if warehouse_id is not None:
        warehouse_db_conf_json = tenant_db.query(MetricConfiguration.config_json).filter(
            MetricConfiguration.metric_name == metric_name_enum,
            MetricConfiguration.warehouse_id == warehouse_id,
            MetricConfiguration.is_active == True
        ).scalar()
        if warehouse_db_conf_json:
            effective_config.update(warehouse_db_conf_json)
            
    return effective_config