from collections import defaultdict
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Dict, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import date, timedelta, datetime, timezone
from sqlalchemy import func
from database.main_db import SessionLocal as MainSessionLocal
from models.main_db import Company, User as UserModel
from models.tenant_db import (DailyStockSnapshot, MetricAlert, MetricName, Transaction, TransactionDetail, MovementType)
from database.dynamic import get_tenant_session
from schemas.metric_schemas import MetricsApiRequest
from services.notification_service import create_notification
from services.get_config import get_effective_metric_config

@contextmanager
def get_main_db():
    """
    Gestor de contexto para sesiones de base de datos.
    Asegura que la sesión se cierre correctamente.
    """
    db: Session = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

def _create_or_update_metric_alert(
    db_session: Session,
    alert_date: date,
    metric_name: MetricName,
    product_id: int,
    warehouse_id: int,
    snapshot_data: Dict[str, Any],
    metric_value_num: Optional[Decimal] = None,
    metric_value_txt: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """Helper para UPSERT en la tabla MetricAlert."""
    alert_payload = {
        "alert_date": alert_date,
        "metric_name": metric_name,
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "physical_stock_at_alert": snapshot_data.get('closing_physical_stock'),
        "available_stock_at_alert": snapshot_data.get('closing_available_stock'),
        "reserved_stock_at_alert": snapshot_data.get('closing_reserved_stock'),
        "metric_value_numeric": metric_value_num,
        "metric_value_text": metric_value_txt,
        "details_json": details or {},
    }

    stmt = pg_insert(MetricAlert).values(**alert_payload)
    update_values = {
        key: getattr(stmt.excluded, key) for key in alert_payload 
        if key not in ['alert_date', 'metric_name', 'product_id', 'warehouse_id']
    }
    update_stmt = stmt.on_conflict_do_update(
        index_elements=['alert_date', 'metric_name', 'product_id', 'warehouse_id'],
        set_=update_values
    )
    db_session.execute(update_stmt)

def evaluate_and_log_metrics_for_snapshot(
    db: Session, 
    current_snapshot_data: Dict[str, Any],
    configs: Dict[MetricName, Dict[str, Any]],
    company_id: int
):
    """
    Evalúa todas las métricas para un snapshot dado y crea/actualiza alertas.
    """
    snap_date = current_snapshot_data['snapshot_date']
    prod_id = current_snapshot_data['product_id']
    wh_id = current_snapshot_data['warehouse_id']
    available_stock = Decimal(current_snapshot_data['closing_available_stock'])

    print("Procesando fecha:")
    print(snap_date)

    # --- STOCK CERO ---
    print("------ Stock Cero -------")
    is_stock_cero = available_stock <= 0
    if is_stock_cero:
        _create_or_update_metric_alert(
            db, snap_date, MetricName.STOCK_CERO, prod_id, wh_id, current_snapshot_data,
            metric_value_num=available_stock,
            details={"message": "Stock disponible es cero o negativo."}
        )
    print("------ Fin Cero -------")

    # --- STOCK CRÍTICO ---
    cfg_critico = configs.get(MetricName.STOCK_CRITICO, {"days_for_avg": 30, "coverage_days_threshold": 3, "stock_qty_threshold": 3})
    avg_sales_start_dt = snap_date - timedelta(days=cfg_critico["days_for_avg"])
    
    total_sold_period = db.query(func.sum(DailyStockSnapshot.quantity_sold)).filter(
        DailyStockSnapshot.product_id == prod_id,
        DailyStockSnapshot.warehouse_id == wh_id,
        DailyStockSnapshot.snapshot_date.between(avg_sales_start_dt, snap_date - timedelta(days=1))
    ).scalar() or 0
    
    avg_daily_sales = Decimal(total_sold_period) / Decimal(cfg_critico["days_for_avg"]) if cfg_critico["days_for_avg"] > 0 else Decimal(0)
    available_stock = Decimal(current_snapshot_data['closing_available_stock'])
    
    days_coverage = available_stock / avg_daily_sales if avg_daily_sales > 0 else Decimal('Infinity')
    
    is_critical = (available_stock <= (avg_daily_sales * Decimal(cfg_critico["coverage_days_threshold"]))) or \
                  (available_stock <= Decimal(cfg_critico["stock_qty_threshold"]))
    if is_critical:
        _create_or_update_metric_alert(
            db, snap_date, MetricName.STOCK_CRITICO, prod_id, wh_id, current_snapshot_data,
            metric_value_num=days_coverage if days_coverage != Decimal('Infinity') else Decimal('999'),
            details={
                "current_available_stock": float(available_stock), 
                "avg_daily_sales_30d": float(avg_daily_sales),
                "coverage_threshold_days": cfg_critico["coverage_days_threshold"],
                "min_qty_threshold": cfg_critico["stock_qty_threshold"]
            }
        )
    
    # --- SOBRE STOCK ---
    cfg_sobrestock = configs.get(MetricName.SOBRE_STOCK, {"days_for_avg": 30, "coverage_days_threshold": 30})
    # Reutilizamos avg_daily_sales si el período es el mismo, sino recalcular.
    # Aquí asumimos que el periodo de avg_sales puede ser diferente.
    avg_sales_start_dt_sobre = snap_date - timedelta(days=cfg_sobrestock["days_for_avg"])
    total_sold_period_sobre = db.query(func.sum(DailyStockSnapshot.quantity_sold)).filter(
        DailyStockSnapshot.product_id == prod_id,
        DailyStockSnapshot.warehouse_id == wh_id,
        DailyStockSnapshot.snapshot_date.between(avg_sales_start_dt_sobre, snap_date - timedelta(days=1))
    ).scalar() or 0
    avg_daily_sales_sobre = Decimal(total_sold_period_sobre) / Decimal(cfg_sobrestock["days_for_avg"]) if cfg_sobrestock["days_for_avg"] > 0 else Decimal(0)
    
    physical_stock = Decimal(current_snapshot_data['closing_physical_stock']) # Usar físico para sobrestock
    days_coverage_physical = physical_stock / avg_daily_sales_sobre if avg_daily_sales_sobre > 0 else Decimal('Infinity')

    is_overstock = False
    if avg_daily_sales_sobre == 0 and physical_stock > 0: # Sin ventas, pero hay stock
        is_overstock = True
    elif avg_daily_sales_sobre > 0 and physical_stock > (avg_daily_sales_sobre * Decimal(cfg_sobrestock["coverage_days_threshold"])):
        is_overstock = True
    
    if is_overstock:
         _create_or_update_metric_alert(
            db, snap_date, MetricName.SOBRE_STOCK, prod_id, wh_id, current_snapshot_data,
            metric_value_num=days_coverage_physical if days_coverage_physical != Decimal('Infinity') else Decimal('999'),
            details={
                "current_physical_stock": float(physical_stock),
                "avg_daily_sales_30d": float(avg_daily_sales_sobre),
                "coverage_threshold_days": cfg_sobrestock["coverage_days_threshold"]
            }
        )

    # --- BAJA ROTACIÓN ---
    cfg_baja_rot = configs.get(MetricName.BAJA_ROTACION, {"days_since_last_sale": 14})
    if physical_stock > 0: # Solo si hay stock
        last_sale_date_val = db.query(func.max(DailyStockSnapshot.snapshot_date)).filter(
            DailyStockSnapshot.product_id == prod_id,
            DailyStockSnapshot.warehouse_id == wh_id,
            DailyStockSnapshot.quantity_sold > 0,
            DailyStockSnapshot.snapshot_date <= snap_date # Hasta la fecha actual del snapshot
        ).scalar()

        days_no_sale = (snap_date - last_sale_date_val).days if last_sale_date_val else 999 # Un número grande si nunca se vendió
        
        if days_no_sale > cfg_baja_rot["days_since_last_sale"]:
            _create_or_update_metric_alert(
                db, snap_date, MetricName.BAJA_ROTACION, prod_id, wh_id, current_snapshot_data,
                metric_value_num=Decimal(days_no_sale),
                details={"last_sale_on": str(last_sale_date_val) if last_sale_date_val else "Never"}
            )

    # --- RECOMENDACIÓN DE COMPRA ---
    cfg_recompra = configs.get(MetricName.RECOMENDACION_COMPRA)
    sales_lookback_start_dt = snap_date - timedelta(days=cfg_recompra["sales_days_for_recommendation"] - 1)
    
    total_sold_lookback = db.query(func.sum(DailyStockSnapshot.quantity_sold)).filter(
        DailyStockSnapshot.product_id == prod_id,
        DailyStockSnapshot.warehouse_id == wh_id,
        DailyStockSnapshot.snapshot_date.between(sales_lookback_start_dt, snap_date)
    ).scalar() or 0

    if available_stock > 0 and total_sold_lookback > 0 and available_stock < total_sold_lookback and company_id == 47:
        units_to_buy = total_sold_lookback - int(available_stock)
        _create_or_update_metric_alert(
            db, snap_date, MetricName.RECOMENDACION_COMPRA, prod_id, wh_id, current_snapshot_data,
            metric_value_num=Decimal(units_to_buy),
            details={
                "current_available_stock": float(available_stock),
                "sales_last_N_days": total_sold_lookback,
                "period_days": cfg_recompra["sales_days_for_recommendation"]
            }
        )
    
    if available_stock < total_sold_lookback and company_id != 47:
        units_to_buy = total_sold_lookback - int(available_stock)
        _create_or_update_metric_alert(
            db, snap_date, MetricName.RECOMENDACION_COMPRA, prod_id, wh_id, current_snapshot_data,
            metric_value_num=Decimal(units_to_buy),
            details={
                "current_available_stock": float(available_stock),
                "sales_last_N_days": total_sold_lookback,
                "period_days": cfg_recompra["sales_days_for_recommendation"]
            }
        )

    # --- DEVOLUCIONES DEL DÍA (Evento) ---
    print("------ DEVOLUCIONES -------")
    print(current_snapshot_data['quantity_returned'])
    if current_snapshot_data['quantity_returned'] > 0:
        print("Creando metrica")
        _create_or_update_metric_alert(
            db, snap_date, MetricName.DEVOLUCIONES, prod_id, wh_id, current_snapshot_data,
            metric_value_num=Decimal(current_snapshot_data['quantity_returned'])
        )
    print("------ FIN DEVOLUCIONES -------")
    
    # --- AJUSTES DEL DÍA (Evento) ---
    if current_snapshot_data['quantity_adjusted'] != 0: # Es el cambio neto
         _create_or_update_metric_alert(
            db, snap_date, MetricName.AJUSTE_STOCK, prod_id, wh_id, current_snapshot_data,
            metric_value_num=Decimal(current_snapshot_data['quantity_adjusted'])
        )

    # --- VENTA SIN STOCK (Evento) ---
    # Si el stock físico al cierre es negativo, es una clara señal.
    print("------ VENTA SIN STOCK -------")
    print(current_snapshot_data['closing_physical_stock'])
    if current_snapshot_data['closing_physical_stock'] < 0:
        print("creando metricas")
        _create_or_update_metric_alert(
            db, snap_date, MetricName.VENTA_SIN_STOCK, prod_id, wh_id, current_snapshot_data,
            metric_value_num=Decimal(current_snapshot_data['closing_physical_stock']) # Stock negativo
        )
    print("------ FIN VENTA SIN STOCK -------")

async def recalculate_stock_snapshots(tenant_session: Optional[Session], product_ids: list[int], warehouse_ids: list[int], start_date: date,company_id:int):
    """
    Recalcula los snapshots de stock en cascada desde una fecha de inicio,
    considerando stock físico y reservado, y procesando transacciones en orden cronológico.
    """
    try:
        today = date.today()
        current_processing_date = start_date
        # Cargar todas las configuraciones de métricas para las sucursales y globales una vez
        all_metric_names = [m for m in MetricName] # Lista de todos los Enum members
        
        while current_processing_date <= today:
            try:
                print(f"--- INICIANDO PROCESAMIENTO PARA FECHA: {current_processing_date} ---")

                all_metric_names = [m for m in MetricName]

                all_configs = {
                    wh_id: {
                        metric_name: get_effective_metric_config(tenant_db=tenant_session,metric_name_enum=metric_name,warehouse_id=wh_id)
                        for metric_name in all_metric_names
                    } for wh_id in warehouse_ids
                }
                global_configs = {
                    metric_name: get_effective_metric_config(tenant_db=tenant_session, metric_name_enum=metric_name,warehouse_id=None)
                    for metric_name in all_metric_names
                }


                for i, product_id_to_process in enumerate(product_ids, start=1):
                    print(f"  Procesando producto {i} de {len(product_ids)}: {product_id_to_process}")
                    for warehouse_id_to_process in warehouse_ids:
                        
                        # 1. Obtener stock de apertura del día anterior
                        opening_physical_stock_for_day = 0
                        opening_reserved_stock_for_day = 0
                        previous_day_date = current_processing_date - timedelta(days=1)
                        
                        previous_snapshot = tenant_session.query(
                            DailyStockSnapshot.closing_physical_stock,
                            DailyStockSnapshot.closing_reserved_stock
                        ).filter(
                            DailyStockSnapshot.snapshot_date == previous_day_date,
                            DailyStockSnapshot.product_id == product_id_to_process,
                            DailyStockSnapshot.warehouse_id == warehouse_id_to_process
                        ).first()

                        
                        if previous_snapshot:
                            opening_physical_stock_for_day = previous_snapshot.closing_physical_stock or 0
                            opening_reserved_stock_for_day = previous_snapshot.closing_reserved_stock or 0

                        ordered_daily_transactions = tenant_session.query(TransactionDetail).join(
                            Transaction, TransactionDetail.transaction_id == Transaction.id
                        ).filter(
                            Transaction.transaction_date == current_processing_date,
                            TransactionDetail.product_id == product_id_to_process,
                            Transaction.warehouse_id == warehouse_id_to_process
                        ).order_by(Transaction.transaction_time).all()

                        # 3. Procesar transacciones secuencialmente
                        current_physical_stock_intra_day = opening_physical_stock_for_day
                        current_reserved_stock_intra_day = opening_reserved_stock_for_day
                        daily_totals = defaultdict(int)
                        

                        print("Tipo de movimiento")
                        for detail in ordered_daily_transactions:
                            stock_before_this_tx = current_physical_stock_intra_day
                            
                            if detail.movement_type == MovementType.VENTA:
                                current_physical_stock_intra_day -= detail.quantity
                                daily_totals['sold'] += detail.quantity
                                if current_reserved_stock_intra_day >= detail.quantity:
                                    current_reserved_stock_intra_day -= detail.quantity
                                    daily_totals['released_from_reservation'] += detail.quantity
                                else:
                                    daily_totals['released_from_reservation'] += current_reserved_stock_intra_day
                                    current_reserved_stock_intra_day = 0

                            elif detail.movement_type == MovementType.RESERVADO:
                                current_reserved_stock_intra_day += detail.quantity
                                daily_totals['newly_reserved'] += detail.quantity

                            elif detail.movement_type == MovementType.DEVOLUCION:
                                current_physical_stock_intra_day += detail.quantity
                                daily_totals['returned'] += detail.quantity

                            elif detail.movement_type == MovementType.RECEPCION:
                                current_physical_stock_intra_day += detail.quantity
                                daily_totals['purchased'] += detail.quantity
                            
                            elif detail.movement_type == MovementType.TRASLADO_ENTRADA:
                                current_physical_stock_intra_day += detail.quantity
                                daily_totals['transfer_in'] += detail.quantity
                            
                            elif detail.movement_type == MovementType.TRASLADO_SALIDA:
                                current_physical_stock_intra_day -= detail.quantity
                                daily_totals['transfer_out'] += detail.quantity

                            elif detail.movement_type in (MovementType.AJUSTE, MovementType.STOCK_INICIAL):
                                target_stock = detail.quantity
                                delta = target_stock - stock_before_this_tx
                                daily_totals['adjusted'] += delta
                                current_physical_stock_intra_day = target_stock

                        # 5. Preparar datos para el UPSERT
                        snapshot_data = {
                            'snapshot_date': current_processing_date,
                            'product_id': product_id_to_process,
                            'warehouse_id': warehouse_id_to_process,
                            'opening_physical_stock': opening_physical_stock_for_day,
                            'opening_reserved_stock': opening_reserved_stock_for_day,
                            'quantity_sold': daily_totals['sold'],
                            'quantity_returned': daily_totals['returned'],
                            'quantity_purchased': daily_totals['purchased'],
                            'quantity_adjusted': daily_totals['adjusted'],
                            'quantity_transfer_in': daily_totals['transfer_in'],
                            'quantity_transfer_out': daily_totals['transfer_out'],
                            'quantity_newly_reserved': daily_totals['newly_reserved'],
                            'quantity_released_from_reservation': daily_totals['released_from_reservation'],
                            'closing_physical_stock': current_physical_stock_intra_day,
                            'closing_reserved_stock': current_reserved_stock_intra_day,
                            'closing_available_stock': current_physical_stock_intra_day - current_reserved_stock_intra_day,
                            'updated_at': datetime.now(timezone.utc)
                        }
                        
                        stmt = pg_insert(DailyStockSnapshot).values(**snapshot_data)
                        update_values = {key: getattr(stmt.excluded, key) for key in snapshot_data if key not in ['snapshot_date', 'product_id', 'warehouse_id']}
                        update_stmt = stmt.on_conflict_do_update(
                            index_elements=['snapshot_date', 'product_id', 'warehouse_id'], set_=update_values
                        )
                        tenant_session.execute(update_stmt)
                        effective_configs = all_configs.get(warehouse_id_to_process, global_configs)
                        evaluate_and_log_metrics_for_snapshot(
                            tenant_session, 
                            snapshot_data,
                            effective_configs,
                            company_id
                        )
                print(f"Guardando cambios para la fecha {current_processing_date}...")
                tenant_session.commit()
                print("Cambios guardados.")

            except Exception as e:
                print(f"Error crítico durante la recalculación para la fecha {current_processing_date}: {e}")
                if tenant_session:
                    tenant_session.rollback()
                raise
            current_processing_date += timedelta(days=1)
            
    except Exception as e:
        print(f"Error crítico durante la recalculación de snapshots: {e}")
        raise