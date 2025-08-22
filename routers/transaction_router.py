from decimal import Decimal
import traceback
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import func
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Set
from collections import defaultdict
from datetime import datetime, date, timedelta

from auth.auth_bearer import get_current_active_user
from database.dynamic import get_tenant_session
from database.main_db import SessionLocal as MainSessionLocal
from models.tenant_db import MovementType, Product, Transaction, TransactionDetail, Warehouse
from routers.metrics_router import _create_report_for_warehouse
from schemas.transaction_schemas import TransactionItemPayload
from models.main_db import Company, MetricDisplayConfiguration, User as UserModel
from services.notification_service import create_notification
from services.snapshot_service import recalculate_stock_snapshots
from services.metric_report_service import generate_and_save_metric_reports

router = APIRouter(
    prefix="/transactions",
    tags=["Transactions"]
)

def get_main_db():
    db = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

async def _process_bulk_transactions_task(
    payload: List[Dict], # Recibimos un dict serializable
    company_id: int,
    tenant_db_url: str,
    user_id: int
):
    """
    Tarea en segundo plano para procesar la carga masiva de transacciones.
    """
    print(f"Iniciando tarea en segundo plano para compañía ID: {company_id}")
    tenant_session: Optional[Session] = None
    main_db: Optional[Session] = None
    
    # Conjuntos para rastrear qué necesita ser recalculado
    affected_dates: Set[date] = set()
    affected_warehouse_ids: Set[int] = set()
    affected_products_ids: Set[int] = set()
    try:
        tenant_session = get_tenant_session(tenant_db_url)
        main_db = MainSessionLocal()

        # 1. Pre-cargar datos existentes para optimizar (Caché en memoria)
        warehouses_cache: Dict[str, Warehouse] = {w.name.lower(): w for w in tenant_session.query(Warehouse).all()}
        products_cache: Dict[str, Product] = {p.code: p for p in tenant_session.query(Product).all()}
        
        # Obtenemos las transacciones existentes para no duplicarlas
        existing_transactions_tuples = tenant_session.query(Transaction.reference_code, Transaction.warehouse_id).all()
        existing_transactions_cache: Set[tuple] = set(existing_transactions_tuples)

        # 2. Agrupar ítems por transacción y sucursal
        grouped_transactions = defaultdict(list)
        for item_dict in payload:
            item = TransactionItemPayload(**item_dict)
            grouped_transactions[(item.sucursal.lower(), item.transaccion)].append(item)
            affected_dates.add(item.fecha_movimiento)

        # 3. Procesar cada grupo de transacciones
        newly_created_warehouses = {}

        for (sucursal_lower, ref_code), items in grouped_transactions.items():
            
            # --- Obtener o Crear Sucursal ---
            warehouse = warehouses_cache.get(sucursal_lower)
            if not warehouse:
                # Si la sucursal no existe, la creamos
                print(f"Creando nueva sucursal: {items[0].sucursal}")
                warehouse = Warehouse(name=items[0].sucursal)
                tenant_session.add(warehouse)
                tenant_session.flush() # Para obtener el ID de la nueva sucursal
                warehouses_cache[sucursal_lower] = warehouse
                newly_created_warehouses[sucursal_lower] = warehouse.id

            affected_warehouse_ids.add(warehouse.id)

            # --- Omitir si la transacción ya existe ---
            if (ref_code, warehouse.id) in existing_transactions_cache:
                print(f"Omitiendo transacción ya existente: {ref_code} en sucursal {warehouse.name}")
                continue

            # --- Crear la Transacción principal ---
            first_item = items[0]
            new_transaction = Transaction(
                reference_code=ref_code,
                transaction_date=first_item.fecha_movimiento,
                transaction_time="00:00:00",
                warehouse_id=warehouse.id
            )

            # --- Crear los Detalles de la Transacción ---
            for item in items:
                # Obtener o Crear Producto
                product = products_cache.get(item.sku)
                if not product:
                    print(f"Creando nuevo producto: {item.sku} - {item.descripcion}")
                    product = Product(
                        code=item.sku,
                        name=item.descripcion,
                        category=item.categoria,
                        cost=Decimal(item.costo) / Decimal(item.cantidad) if item.cantidad > 0 else Decimal(0)
                    )
                    tenant_session.add(product)
                    tenant_session.flush() # Para obtener el ID
                    products_cache[item.sku] = product

                # Crear el detalle
                detail = TransactionDetail(
                    product_id=product.id,
                    quantity=item.cantidad,
                    unit_cost=product.cost,
                    movement_type=MovementType(item.tipo.lower()),
                    transaction=new_transaction
                )
                tenant_session.add(detail)
                affected_products_ids.add(product.id)
        
        tenant_session.commit()
        print(f"Carga de {len(grouped_transactions)} transacciones completada.")

        # 4. Tareas de Post-Procesamiento
        if affected_dates:
            start_date = min(affected_dates)
            end_date = max(affected_dates)
            
            # Recalcular snapshots de stock para el rango de fechas afectado
            print(f"Iniciando recálculo de snapshots desde {start_date} hasta {end_date}.")
            await recalculate_stock_snapshots(company_id=company_id, tenant_session=tenant_session, start_date=start_date, warehouse_ids=affected_warehouse_ids, product_ids=affected_products_ids)
            print("Recálculo de snapshots completado.")

            # Regenerar los reportes de métricas para las fechas y sucursales afectadas
            await generate_and_save_metric_reports(
                tenant_session=tenant_session,
                main_db=main_db,
                company_id=company_id,
                dates_to_process=list(affected_dates),
                warehouse_ids=list(affected_warehouse_ids)
            )
         
        display_configs_from_db = main_db.query(MetricDisplayConfiguration).filter_by(company_id=company_id).all()
        display_configs_map = {config.metric_id: config for config in display_configs_from_db}

        current_date = start_date
        while current_date <= end_date:
            for warehouse_id in affected_warehouse_ids:
                print(f"Iniciando recálculo de snapshots para {current_date} en warehouse {warehouse_id}")
                await _create_report_for_warehouse(
                    tenant_session=tenant_session,
                    main_db=main_db,
                    company_id=company_id,
                    target_date=current_date,
                    display_configs_map=display_configs_map,
                    target_warehouse_id=warehouse_id,
                    rp_sistemas_session=None
                )
            current_date += timedelta(days=1)
        # 5. Notificar al usuario
        #await create_notification(
        #    db=main_db,
        #    users_to_notify=[user_id],
        #    company_id=company_id,
        #    message=f"La carga masiva de {len(grouped_transactions)} transacciones se ha completado exitosamente."
        #)

    except Exception as e:
        if tenant_session: tenant_session.rollback()
        traceback.print_exc()
        # Notificar error al usuario
        #if main_db:
        #    await create_notification(
        #        db=main_db,
        #        users_to_notify=[user_id],
        #        company_id=company_id,
        #        message=f"Error durante la carga masiva de transacciones. Por favor, revise los datos o contacte a soporte. Error: {e}"
        #    )
    finally:
        if tenant_session: tenant_session.close()
        if main_db: main_db.close()
        print(f"Tarea en segundo plano para compañía ID: {company_id} finalizada.")


@router.post("/bulk", status_code=status.HTTP_202_ACCEPTED)
async def process_bulk_transactions(
    payload: List[TransactionItemPayload],
    background_tasks: BackgroundTasks,
    company_id: int = Query(..., description="ID de la empresa para la cual procesar transacciones"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Procesa una carga masiva de transacciones de inventario para una empresa específica.
    La tarea se ejecuta en segundo plano. El usuario será notificado al completarse.
    """
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized for this company")
    
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail=f"La configuración de la base de datos para la empresa ID {company_id} está incompleta.")
    
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    
    # Convertimos el payload a un diccionario para que sea serializable por la tarea en segundo plano
    payload_dict = [p.dict() for p in payload]
    print("Iniciando 2 plano")
    # Añadimos la tarea al fondo
    background_tasks.add_task(
        _process_bulk_transactions_task, 
        payload=payload_dict, 
        company_id=company_id, 
        tenant_db_url=tenant_db_url,
        user_id=current_user.id
    )

    return {"message": "Proceso de carga masiva iniciado. Recibirás una notificación cuando finalice."}