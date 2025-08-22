from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
from database.main_db import SessionLocal as MainSessionLocal
from database.dynamic import get_tenant_session
from models.bsale_db import Bsale_Office
from models.main_db import Company
from models.tenant_db import Warehouse
from schemas.warehouse_schemas import WarehouseCreate, WarehouseOut, WarehouseUpdate
from auth.auth_bearer import get_current_active_user
from models.main_db import User as UserModel

def get_main_db():
    db = MainSessionLocal()
    try:
        yield db
    finally:
        db.close()

router = APIRouter(
    prefix="/warehouses",
    tags=["Warehouses (Sucursales)"]
)


@router.get("", response_model=List[WarehouseOut])
async def get_warehouses_by_company(
    company_id: int = Query(..., description="ID de la empresa para la cual obtener las sucursales"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Obtiene todas las sucursales para una empresa. Si la empresa está integrada
    con Bsale, prioriza la tabla 'bsale_office', pero recurre a 'warehouses'
    si la primera está vacía o no aplica.
    """
    # 1. Obtener datos de la empresa, incluyendo su integración, para la validación.
    company = main_db.query(Company).options(
        joinedload(Company.integration)
    ).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=404, detail=f"Empresa con ID {company_id} no encontrada.")

    # 2. Verificar autorización del usuario (sin cambios)
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(
            status_code=403,
            detail=f"No está autorizado para acceder a las sucursales de la empresa con ID {company_id}."
        )

    # 3. Verificar configuración de la base de datos del inquilino (sin cambios)
    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(
            status_code=500,
            detail=f"La configuración de la base de datos para la empresa ID {company_id} está incompleta."
        )

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None
    
    try:
        tenant_session = get_tenant_session(tenant_db_url)
        warehouses = [] # Inicializar como lista vacía

        # --- LÓGICA CONDICIONAL DE FALLBACK ---
        
        # Primero, intentar obtener sucursales desde Bsale si la integración es la correcta
        if company.integration and company.integration.id == 1:
            print(f"Compañía {company_id} es Bsale. Obteniendo sucursales desde bsale_office.")
            warehouses = tenant_session.query(Warehouse).all()

        # Si 'warehouses' sigue vacía (porque no es Bsale, o su tabla está vacía),
        # consultamos la tabla estándar como respaldo.
        if not warehouses:
            print("No se encontraron sucursales de Bsale o no aplica. Usando tabla 'warehouses' como fallback.")
            warehouses = tenant_session.query(Warehouse).all()
        
        # --- FIN DE LA LÓGICA ---
        
        return warehouses

    except Exception as e:
        print(f"Error al acceder a la base de datos del inquilino para la empresa ID {company_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo conectar o consultar la base de datos de sucursales."
        )
    finally:
        if tenant_session:
            tenant_session.close()

@router.post("", status_code=status.HTTP_201_CREATED, response_model=WarehouseOut)
async def create_warehouse_for_company(
    warehouse_in: WarehouseCreate,
    company_id: int = Query(..., description="ID de la empresa donde se creará la sucursal"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Crea una nueva sucursal para una empresa específica.
    """
    # 1. Obtener compañía y verificar permisos (igual que en el endpoint GET)
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Empresa con ID {company_id} no encontrada.")

    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="No autorizado para crear sucursales en esta empresa.")

    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail=f"La configuración de la base de datos para la empresa ID {company_id} está incompleta.")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        # 2. (Opcional pero recomendado) Verificar si ya existe una sucursal con el mismo nombre
        existing_warehouse = tenant_session.query(Warehouse).filter(Warehouse.name == warehouse_in.name).first()
        if existing_warehouse:
            raise HTTPException(status_code=409, detail=f"Una sucursal con el nombre '{warehouse_in.name}' ya existe en esta empresa.")

        # 3. Crear, guardar y devolver la nueva sucursal
        new_warehouse = Warehouse(**warehouse_in.model_dump())
        tenant_session.add(new_warehouse)
        tenant_session.commit()
        tenant_session.refresh(new_warehouse)
        return new_warehouse
    except Exception as e:
        print(f"Error al crear sucursal para la empresa ID {company_id}: {e}")
        raise HTTPException(status_code=503, detail=f"No se pudo acceder a la base de datos de la empresa ID {company_id}.")
    finally:
        if tenant_session:
            tenant_session.close()

@router.put("/{warehouse_id}", response_model=WarehouseOut)
async def update_warehouse_in_company(
    warehouse_id: int,
    warehouse_in: WarehouseUpdate,
    company_id: int = Query(..., description="ID de la empresa a la que pertenece la sucursal"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Actualiza una sucursal existente dentro de una empresa específica.
    """
    # 1. Obtener compañía y verificar permisos (misma lógica que antes)
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Empresa con ID {company_id} no encontrada.")
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="No autorizado para modificar sucursales en esta empresa.")

    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail=f"La configuración de la base de datos para la empresa ID {company_id} está incompleta.")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        # 2. Buscar la sucursal a actualizar en la base de datos del inquilino
        db_warehouse = tenant_session.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not db_warehouse:
            raise HTTPException(status_code=404, detail=f"Sucursal con ID {warehouse_id} no encontrada en la empresa ID {company_id}.")

        # 3. Actualizar el objeto con los datos proporcionados
        update_data = warehouse_in.model_dump(exclude_unset=True) # exclude_unset=True para no sobreescribir campos no enviados
        for key, value in update_data.items():
            setattr(db_warehouse, key, value)
        
        tenant_session.add(db_warehouse)
        tenant_session.commit()
        tenant_session.refresh(db_warehouse)
        return db_warehouse
    except Exception as e:
        print(f"Error al actualizar sucursal {warehouse_id} para la empresa ID {company_id}: {e}")
        raise HTTPException(status_code=503, detail=f"No se pudo acceder a la base de datos de la empresa ID {company_id}.")

@router.delete("/{warehouse_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_warehouse(
    warehouse_id: int,
    company_id: int = Query(..., description="ID de la empresa a la que pertenece la sucursal"),
    main_db: Session = Depends(get_main_db),
    current_user: UserModel = Depends(get_current_active_user)
):
    """
    Elimina una sucursal existente.
    No permite eliminar la última sucursal de una empresa.
    """
    # 1. Validar compañía y permisos
    company = main_db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Empresa con ID {company_id} no encontrada.")

    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="No autorizado para eliminar sucursales en esta empresa.")

    if not all([company.db_user, company.db_password, company.db_host, company.db_name]):
        raise HTTPException(status_code=500, detail=f"La configuración de la base de datos para la empresa ID {company_id} está incompleta.")

    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session: Optional[Session] = None

    try:
        tenant_session = get_tenant_session(tenant_db_url)
        
        # 2. REGLA DE NEGOCIO: No permitir borrar la última sucursal
        warehouse_count = tenant_session.query(func.count(Warehouse.id)).scalar()
        if warehouse_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No se puede eliminar la última sucursal. Debe existir al menos una."
            )
            
        # 3. Buscar la sucursal a eliminar
        db_warehouse = tenant_session.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not db_warehouse:
            raise HTTPException(status_code=404, detail=f"Sucursal con ID {warehouse_id} no encontrada.")
            
        # 4. Eliminar y guardar cambios
        tenant_session.delete(db_warehouse)
        tenant_session.commit()
        
        # 5. Devolver respuesta vacía con código 204
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except HTTPException as http_exc:
        if tenant_session: tenant_session.rollback()
        raise http_exc
    except Exception as e:
        if tenant_session: tenant_session.rollback()
        print(f"Error al eliminar sucursal {warehouse_id} para la empresa ID {company_id}: {e}")
        raise HTTPException(status_code=503, detail=f"No se pudo acceder a la base de datos de la empresa ID {company_id}.")
    finally:
        if tenant_session:
            tenant_session.close()