from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import date, timedelta, datetime, timezone
from typing import List, Optional, Dict, Any
from decimal import Decimal
from database.main_db import SessionLocal as MainSessionLocal, get_db
from database.dynamic import get_tenant_session
from models.main_db import Company, User as UserModel
from models.tenant_db import (
    MetricName, Warehouse, Product, MetricConfiguration
)
from schemas.metric_schemas import (
    MetricsApiResponse, MetricGridItem, MetricDataItem, MetricsApiRequest, 
    OverviewResponse, OverviewDataPoint,
    CounterItem, DistributionItem, EscalationItem, EscalationProductItem
)
from auth.auth_bearer import get_current_active_user
from services import bsale_metrics_calculator
from services.bsale_etl_service import run_full_bsale_etl
from services.get_config import get_effective_metric_config

# --- Dependencia para la Sesión de la DB Principal ---
def get_main_db():
    db = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

router = APIRouter(
    prefix="/bsale-metrics",
    tags=["BSale - Metrics & Reporting"]
)


@router.post("/{company_id}/run-etl", status_code=status.HTTP_202_ACCEPTED)
async def trigger_bsale_etl_task(
    company_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Inicia el proceso ETL para una compañía integrada con Bsale.

    Este proceso se ejecuta en segundo plano y consiste en:
    1. Sincronizar sucursales y productos de Bsale con las tablas internas.
    2. Calcular todas las métricas del día basadas en los datos de Bsale.
    3. Guardar los resultados en la tabla de Alertas de Métricas.
    """
    # Validar que la compañía existe y que el usuario tiene permisos
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Compañía no encontrada.")
        
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="No autorizado para esta acción.")
    
    if not company.integration_id == 1: # Asumiendo que 1 es el ID para Bsale
        raise HTTPException(status_code=400, detail="Esta acción solo es válida para compañías con integración Bsale.")
    # Programar la tarea de fondo, pasándole solo el company_id
    background_tasks.add_task(run_full_bsale_etl, company_id=company_id)
    
    return {"message": "El proceso de sincronización y cálculo de alertas para Bsale ha sido iniciado en segundo plano."}

@router.get("/{company_id}", response_model=MetricsApiResponse)
async def get_bsale_company_metrics(
    company_id: int,
    report_params: MetricsApiRequest = Depends(),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Obtiene un reporte completo de métricas de inventario basado en los datos
    sincronizados desde Bsale.
    """
    # 1. Validar Compañía, Permisos y Configuración de DB
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

        # 2. Determinar la sucursal a filtrar (si aplica)
        target_warehouse_id: Optional[int] = None
        if report_params.store:
            warehouse_obj = tenant_session.query(Warehouse.id).filter(func.lower(Warehouse.name) == report_params.store.lower()).first()
            if not warehouse_obj:
                raise HTTPException(status_code=404, detail=f"Sucursal '{report_params.store}' no encontrada.")
            target_warehouse_id = warehouse_obj[0]

        # 3. Cargar las configuraciones de métricas
        configs = {
            name: get_effective_metric_config(tenant_session, name, target_warehouse_id)
            for name in MetricName
        }

        all_warehouses = tenant_session.query(Warehouse).all()
        warehouse_map = {w.name: w.id for w in all_warehouses}

        all_products = tenant_session.query(Product).all()
        product_map = {p.code: {"id": p.id, "cost": p.cost or 0} for p in all_products}

        # 4. Ejecutar todas las funciones de cálculo de métricas desde el servicio
        print("Ejecutando cálculos de métricas de Bsale...")
        stock_cero_data = bsale_metrics_calculator.get_bsale_stock_cero(
            db=tenant_session, 
            end_date=report_params.dateEnd, 
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        baja_rotacion_data = bsale_metrics_calculator.get_bsale_baja_rotacion(
            db=tenant_session, 
            end_date=report_params.dateEnd, 
            config=configs[MetricName.BAJA_ROTACION], 
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        stock_critico_data = bsale_metrics_calculator.get_bsale_stock_critico(
            db=tenant_session, 
            end_date=report_params.dateEnd, 
            config=configs[MetricName.STOCK_CRITICO], 
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        sobre_stock_data = bsale_metrics_calculator.get_bsale_sobre_stock(
            db=tenant_session, 
            end_date=report_params.dateEnd, 
            config=configs[MetricName.SOBRE_STOCK], 
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        recompra_data = bsale_metrics_calculator.get_bsale_recomendacion_compra(
            db=tenant_session, 
            end_date=report_params.dateEnd, 
            config=configs[MetricName.RECOMENDACION_COMPRA], 
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        devoluciones_data = bsale_metrics_calculator.get_bsale_devoluciones(tenant_session, report_params.dateInit, report_params.dateEnd, target_warehouse_id)
        ajustes_data = bsale_metrics_calculator.get_bsale_ajuste_stock(
            db=tenant_session,
            api_key=company.api_key,
            start_date=report_params.dateInit,
            end_date=report_params.dateEnd,
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        venta_sin_stock_data: List[MetricDataItem] = bsale_metrics_calculator.get_bsale_venta_sin_stock(
            db=tenant_session, 
            end_date=report_params.dateEnd,
            warehouse_map=warehouse_map,
            product_map=product_map,
            warehouse_id=target_warehouse_id
        )
        print("Cálculos completados.")

        # 5. Ensamblar la respuesta
        
        # --- Overview (Gráfico) ---
        # Esta sección puede ser lenta. En producción, considera pre-calcularla o simplificarla.
        overview_response = OverviewResponse(data=[]) # Placeholder por ahora para mantener la velocidad
        
        # --- Grid ---
        metric_results_grid = [
            MetricGridItem(name="Stock Cero", metricId=MetricName.STOCK_CERO, data=stock_cero_data),
            MetricGridItem(name="Baja Rotación", metricId=MetricName.BAJA_ROTACION, data=baja_rotacion_data),
            MetricGridItem(name="Stock Crítico", metricId=MetricName.STOCK_CRITICO, data=stock_critico_data),
            MetricGridItem(name="Sobre Stock", metricId=MetricName.SOBRE_STOCK, data=sobre_stock_data),
            MetricGridItem(name="Ajuste de Stock", metricId=MetricName.AJUSTE_STOCK, data=ajustes_data),
            MetricGridItem(name="Devoluciones", metricId=MetricName.DEVOLUCIONES, data=devoluciones_data),
            MetricGridItem(name="Compra Sugerida", metricId=MetricName.RECOMENDACION_COMPRA, data=recompra_data),
            MetricGridItem(name="Venta Sin Stock", metricId=MetricName.VENTA_SIN_STOCK, data=venta_sin_stock_data),
        ]
        
        # --- Counters ---
        all_counters: List[CounterItem] = []
        metric_data_map = {
            "Productos con Stock Cero": {"data": stock_cero_data, "info": "Productos con stock disponible <= 0."},
            "Productos con Baja Rotación": {"data": baja_rotacion_data, "info": f"Productos sin venta en los últimos {configs[MetricName.BAJA_ROTACION]['days_since_last_sale']} días."},
            "Productos con Stock Crítico": {"data": stock_critico_data, "info": "Productos con baja cobertura de stock según ventas."},
            "Sobre Stock": {"data": sobre_stock_data, "info": "Productos con cobertura de stock excesiva."},
            "Devoluciones": {"data": devoluciones_data, "info": "Total de productos con devoluciones en el período."},
            "Ajuste de Stock": {"data": ajustes_data, "info": "Movimientos de ajuste de stock en el período."},
            "Compra Sugerida": {"data": recompra_data, "info": "Sugerencias de compra basadas en rotación y stock actual."},
            "Venta Sin Stock": {"data": venta_sin_stock_data, "info": "Instancias donde el stock físico fue negativo."},
        }

        for name, values in metric_data_map.items():
            data = values["data"]
            total_units = 0
            total_value = Decimal(0)
            print(data)
            unique_products_in_metric = {item.product_sku: Decimal(item.cost or 0) for item in data}

            if name == "Productos con Stock Cero":
                total_units = 0
                total_value = sum(unique_products_in_metric.values()) # Valor de los SKUs fuera de stock
            else:
                for item in data:
                    cost = Decimal(item.cost or 0)
                    units = 0
                    if name in ["Productos con Stock Crítico"]:
                        units = item.available_stock or 0
                    elif name in ["Productos con Baja Rotación", "Sobre Stock", "Venta Sin Stock"]:
                        units = abs(item.physical_stock or 0)
                    elif name in ["Devoluciones", "Ajuste de Stock", "Compra Sugerida"]:
                        units = int(Decimal(item.metric_value or 0))
                    
                    total_units += units
                    total_value += Decimal(units) * cost
            
            all_counters.append(CounterItem(
                name=name,
                quantity=len(unique_products_in_metric),
                information=values["info"],
                amount=total_units,
                price=float(total_value)
            ))
        
        # --- Distributions ---
        all_distributions = [DistributionItem(name=c.name, price=c.price) for c in all_counters]

        # --- Products (Escalamiento) ---
        all_escalation_items = [] # Dejado como TODO, ya que requiere una lógica de "días consecutivos" más compleja
                                   # que idealmente se calcularía y guardaría en MetricAlert.

        # --- Ensamblar Respuesta Final ---
        return MetricsApiResponse(
            grid=metric_results_grid,
            overview=overview_response,
            counters=all_counters,
            distributions=all_distributions,
            products=all_escalation_items 
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        import traceback
        print("------------------------------------------------------------")
        print(f"ERROR GENERAL en get_bsale_company_metrics para empresa ID {company_id}:")
        traceback.print_exc()
        print("------------------------------------------------------------")
        # El bloque with...begin() dentro de los helpers de bsale_metrics_calculator debería manejar el rollback.
        raise HTTPException(status_code=500, detail=f"Error interno al generar reporte de métricas de Bsale: {str(e)}")
    finally:
        if tenant_session:
            tenant_session.close()