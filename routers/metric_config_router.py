from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func # Para func.lower si es necesario
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone # Para updated_at

# Dependencias y modelos
from database.main_db import SessionLocal as MainSessionLocal
from database.dynamic import get_tenant_session
from models.main_db import Company, User as UserModel
from models.tenant_db import MetricConfiguration, MetricName, Warehouse # Importa MetricConfiguration
from schemas.metric_schemas import MetricConfigurationCreate, MetricConfigurationOut, MetricConfigurationUpdate
from auth.auth_bearer import get_current_active_user # O get_current_active_superuser si es solo para admins
from fastapi_cache.decorator import cache

# Dependencia para la Sesión de la DB Principal
def get_main_db():
    db = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

router = APIRouter(
    prefix="/metric-configurations", # Un prefijo claro para estos endpoints
    tags=["Metric Configurations"]
)

@router.post("", response_model=MetricConfigurationOut, status_code=status.HTTP_201_CREATED)
async def create_or_update_metric_configuration(
    config_in: MetricConfigurationCreate,
    company_id: int = Query(..., description="ID de la empresa para esta configuración"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user) # TODO: Considerar permisos de superusuario/admin
):
    """
    Crea una nueva configuración de métrica o actualiza una existente
    basada en la combinación única de (metric_name, warehouse_id).
    Si warehouse_id es None, se aplica a la configuración global de la empresa para esa métrica.
    """

    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # TODO: Lógica de autorización más granular (ej. solo ciertos roles pueden modificar configs)
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated: # Ejemplo de permiso
        raise HTTPException(status_code=403, detail="Not authorized to configure metrics for this company")

    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="Database configuration for company is incomplete.")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None
    
    try:
        tenant_session = get_tenant_session(tenant_db_url)

        # Validar que warehouse_id (si se provee) exista en esta tenant_db
        print("validando sucursal")
        if config_in.warehouse_id is not None:
            warehouse = tenant_session.query(Warehouse.id).filter(Warehouse.id == config_in.warehouse_id).first()
            if not warehouse:
                raise HTTPException(status_code=404, detail=f"Warehouse ID {config_in.warehouse_id} not found for this company.")

        # Lógica UPSERT: buscar por la combinación única
        print("buscar por la combinación única")
        db_config = tenant_session.query(MetricConfiguration).filter(
            MetricConfiguration.metric_name == config_in.metric_name,
            MetricConfiguration.warehouse_id == config_in.warehouse_id # Funciona bien si warehouse_id es None o un int
        ).first()

        if db_config: # Actualizar existente
            print("Actualizar existente")
            db_config.config_json = config_in.config_json
            db_config.is_active = config_in.is_active
            db_config.updated_at = datetime.now(timezone.utc)
            status_to_return = status.HTTP_200_OK # No es estándar devolver 201 en update, pero FastAPI lo maneja
        else: # Crear nueva
            print("Crear nueva")
            db_config = MetricConfiguration(
                metric_name=config_in.metric_name,
                warehouse_id=config_in.warehouse_id,
                config_json=config_in.config_json,
                is_active=config_in.is_active
                # updated_at y created_at usarán server_default si no se pasan aquí
            )
            tenant_session.add(db_config)
            status_to_return = status.HTTP_201_CREATED
        
        tenant_session.commit()
        tenant_session.refresh(db_config)
        
        # FastAPI no permite cambiar el status_code dinámicamente así de fácil.
        # Se usará el status_code del decorador (201). Para UPSERT, a veces se devuelve 200 o 201.
        return db_config

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        if tenant_session: tenant_session.rollback()
        print(f"Error en configuración de métrica: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno al guardar configuración: {str(e)}")
    finally:
        if tenant_session:
            tenant_session.close()

@router.get("", response_model=List[MetricConfigurationOut])
@cache(namespace="metric_configurations", expire=300)
async def list_metric_configurations(
    company_id: int = Query(...),
    warehouse_id: Optional[int] = Query(None, description="Filtrar por ID de sucursal. No enviar para configs globales/todas."),
    metric_name: Optional[MetricName] = Query(None, description="Filtrar por nombre de métrica."),
    is_active: Optional[bool] = Query(None, description="Filtrar por estado activo."),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    # ... (Validación de compañía, permisos, conexión a DB del inquilino como en POST) ...
    company = main_db.query(Company).filter(Company.id == company_id).first() # Repetido, considera un helper/dependencia
    if not company: raise HTTPException(status_code=404, detail="Company not found")
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="DB config incomplete.")
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        query = tenant_session.query(MetricConfiguration)
        
        if warehouse_id is not None: # Si se provee, filtra por esa sucursal
            query = query.filter(MetricConfiguration.warehouse_id == warehouse_id)
        # Si se quiere explícitamente las globales, se podría añadir un param ?global_only=true
        # y aquí: query = query.filter(MetricConfiguration.warehouse_id == None)

        if metric_name is not None:
            query = query.filter(MetricConfiguration.metric_name == metric_name)
        if is_active is not None:
            query = query.filter(MetricConfiguration.is_active == is_active)
            
        configs = query.order_by(MetricConfiguration.warehouse_id, MetricConfiguration.metric_name).all()
        return configs
    finally:
        if tenant_session: tenant_session.close()

@router.get("/{config_id}", response_model=MetricConfigurationOut)
async def get_metric_configuration_by_id(
    config_id: int,
    company_id: int = Query(...), # Necesario para saber a qué DB de inquilino conectar
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    # ... (Validación de compañía, permisos, conexión a DB del inquilino) ...
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company: raise HTTPException(status_code=404, detail="Company not found") # etc.
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="DB config incomplete.")
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None
    
    try:
        tenant_session = get_tenant_session(tenant_db_url)
        config = tenant_session.query(MetricConfiguration).filter(MetricConfiguration.id == config_id).first()
        if not config:
            raise HTTPException(status_code=404, detail="Metric configuration not found")
        return config
    finally:
        if tenant_session: tenant_session.close()

@router.put("/{config_id}", response_model=MetricConfigurationOut)
async def update_metric_configuration_by_id(
    config_id: int,
    config_in: MetricConfigurationUpdate, # Esquema para actualización parcial
    company_id: int = Query(...),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user) # TODO: Permisos
):
    # ... (Validación de compañía, permisos, conexión a DB del inquilino) ...
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company: raise HTTPException(status_code=404, detail="Company not found") # etc.
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="DB config incomplete.")
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        db_config = tenant_session.query(MetricConfiguration).filter(MetricConfiguration.id == config_id).first()
        if not db_config:
            raise HTTPException(status_code=404, detail="Metric configuration not found to update")

        update_data = config_in.model_dump(exclude_unset=True)
        if not update_data: # Si no se envía nada para actualizar
             raise HTTPException(status_code=400, detail="No update data provided")

        for key, value in update_data.items():
            setattr(db_config, key, value)
        db_config.updated_at = datetime.now(timezone.utc)
        
        tenant_session.commit()
        tenant_session.refresh(db_config)
        return db_config
    finally:
        if tenant_session: tenant_session.close()

@router.delete("/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_metric_configuration(
    config_id: int,
    company_id: int = Query(...),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user) # TODO: Permisos
):
    # ... (Validación de compañía, permisos, conexión a DB del inquilino) ...
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company: raise HTTPException(status_code=404, detail="Company not found") # etc.
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail="DB config incomplete.")
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        db_config = tenant_session.query(MetricConfiguration).filter(MetricConfiguration.id == config_id).first()
        if not db_config:
            raise HTTPException(status_code=404, detail="Metric configuration not found to delete")
        
        # Opción 1: Hard Delete
        tenant_session.delete(db_config)
        
        # Opción 2: Soft Delete (si prefieres mantener el registro pero marcarlo inactivo)
        #db_config.is_active = False
        #db_config.updated_at = datetime.now(timezone.utc)
        tenant_session.commit()
        return # Para 204 No Content
    finally:
        if tenant_session: tenant_session.close()