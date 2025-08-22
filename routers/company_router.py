import traceback
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile, status, Query
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typing import List, Optional
from database.dynamic import get_tenant_session
from database.main_db import SessionLocal, get_db
from models.bsale_db import Bsale_Office, TenantBase
from models.main_db import Company, Integration, User
from models.tenant_db import Warehouse
from routers.bsale import get_main_db
from schemas.company_schemas import CompanyIntegrationUpdate, CompanyResponse, CompanyUpdate, IntegrationOut
from crud import company_crud
from auth.auth_bearer import get_current_active_user, get_current_active_superuser
from schemas.user_schemas import UserResponse
from services.bsale_api_service import get_bsale_users
from services.email_service import send_email
from services.tenant_manager import delete_company_and_resources
from utils.bsale import company_bsale
from utils.s3_uploader import upload_file_to_s3
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/companies", tags=["Companies"])

async def _send_bsale_webhook_activation_email(company: Company, api_key: str):
    """
    Tarea de fondo que:
    1. Obtiene datos de Bsale (incluyendo el cpnId).
    2. Actualiza el registro de la compañía con el external_id (cpnId).
    3. Envía el email de solicitud de activación de webhook.
    """
    print("Iniciando tarea de fondo para activación de webhook de Bsale...")
    try:
        main_db: Session = SessionLocal()

        # 1. Obtener información de la compañía desde Bsale
        bsale_company = await company_bsale.get_data(api_key)
        if not bsale_company:
            print("Error en tarea de fondo: No se pudo obtener la información de la compañía desde Bsale.")
            return
        # 2. Obtener usuarios de Bsale
        users = get_bsale_users(api_key)
        
        company_to_update = main_db.query(Company).filter_by(id=company.id).first()
        if not company_to_update:
            raise Exception(f"No se encontró la compañía con ID {company.id} para actualizar el external_id.")
        
        company_to_update.external_id = str(bsale_company['id']) # Guardamos el cpnId de Bsale
        main_db.commit()
        print(f"ID externo '{bsale_company['id']}' guardado para la compañía ID {company.id}.")


        # 3. Encontrar el primer usuario con ID >= 2
        contact_user = None
        if users:
            # Ordenar por ID para encontrar el más cercano a 2
            sorted_users = sorted(users, key=lambda u: u.get('id', float('inf')))
            for user in sorted_users:
                if user.get('id', 0) >= 2:
                    contact_user = user
                    break
        
        if not contact_user:
            print("Error en tarea de fondo: No se encontró un usuario de contacto válido (ID >= 2) en Bsale.")
            return

        # 4. Construir el cuerpo del email
        subject = f"Activación de Webhooks para {bsale_company['name']}"
        body_html = f"""
        <html>
        <body>
            <h2>Solicitud de Activación de Webhooks</h2>
            <p>Por favor, activar los webhooks siguientes webhook (Noticifaciones base) para la siguiente cuenta de Bsale:</p>
            <ul>
                <li><strong>Documento</li>
                <li><strong>Stock</li>
                <li><strong>Producto</li>
                <li><strong>Variante</li>
                <li><strong>Precio</li>
            <ul/>
            <p>Datos de la empresa</p>
            <ul>
                <li><strong>Nombre Empresa:</strong> {bsale_company['name']}</li>
                <li><strong>RUT/Código:</strong> {bsale_company['name']}</li>
                <li><strong>ID Compañía Bsale (cpnId):</strong> {bsale_company['id']}</li>
            </ul>
            <p><strong>URL del Webhook a configurar:</strong></p>
            <p><code>https://apiv2.inventaria.cl/webhook_bsale</code></p>
            <hr>
            <p><strong>Datos del Usuario de Contacto en Bsale:</strong></p>
            <ul>
                <li><strong>Nombre:</strong> {contact_user.get('firstName')} {contact_user.get('lastName')}</li>
                <li><strong>Email:</strong> {contact_user.get('email')}</li>
            </ul>
            <p>Gracias.</p>
        </body>
        </html>
        """

        # 5. Enviar el email
        send_email(
            to_addresses=["jairojairjason@gmail.com","ayuda@bsale.app","slewinr@quot.cl"],
            subject=subject,
            body_html=body_html
        )
        print("Realizando migraciónes...")
        db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        engine = create_engine(db_url)
        TenantBase.metadata.create_all(bind=engine)
        print("Migración completada")
    except Exception as e:
        print(f"--- ERROR CRÍTICO EN TAREA DE FONDO (Email de Activación) ---")
        traceback.print_exc()
    finally:
        main_db.close()

@router.get("/{company_id}/integration", response_model=IntegrationOut)
async def get_company_integration(
    company_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Obtiene la integración actualmente configurada para una compañía específica.
    """
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Compañía no encontrada.")
        
    if not company.integration:
        raise HTTPException(status_code=404, detail="La compañía no tiene ninguna integración configurada.")
        
    return company.integration

@router.put("/{company_id}/integration", response_model=CompanyResponse)
async def update_company_integration(
    company_id: int,
    payload: CompanyIntegrationUpdate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Establece o actualiza la integración y la API Key para una compañía.
    """
    company = db.query(Company).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Compañía no encontrada.")

    # Validar que el integration_id del payload existe (si se proporciona)
    if payload.integration_id is not None:
        integration = db.query(Integration.id).filter_by(id=payload.integration_id, is_active=True).first()
        if not integration:
            raise HTTPException(status_code=404, detail=f"La integración con ID {payload.integration_id} no existe o no está activa.")
    
    company.integration_id = payload.integration_id
    company.api_key = payload.api_key
    db.commit()
    db.refresh(company)
    if payload.integration_id == 1 and payload.api_key:
        print(f"Detectada integración con Bsale para la compañía ID {company_id}. Programando email de activación.")
        background_tasks.add_task(_send_bsale_webhook_activation_email, company=company , api_key=payload.api_key)
    return company

@router.put("/{company_id}", response_model=CompanyResponse)
def update_company(
    company_id: int,
    db: Session = Depends(get_db),
    name: Optional[str] = Form(None),
    rut: Optional[str] = Form(None),
    selectedAvatar: Optional[UploadFile] = File(None) 
):
    """
    Actualiza una empresa. Acepta datos de texto y un archivo opcional en una sola petición multipart/form-data.
    """
    # 1. Buscar la empresa
    db_company = db.query(Company).filter(Company.id == company_id).first()
    if db_company is None:
        raise HTTPException(status_code=404, detail=f"Company with id {company_id} not found")

    # 2. Crear un diccionario para almacenar los cambios
    update_data = {}
    if name is not None:
        update_data['name'] = name
    if rut is not None:
        update_data['rut'] = rut

    # 3. Detectar si se envió un archivo. Si es así, subirlo y guardar la URL.
    if selectedAvatar:
        if selectedAvatar.filename:
            avatar_url = upload_file_to_s3(selectedAvatar)
            update_data['selectedAvatar'] = avatar_url
    elif 'selectedAvatar' in update_data and update_data['selectedAvatar'] is None:
         update_data['selectedAvatar'] = None

    # 4. Si no hay nada que actualizar, informa al cliente.
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update provided.")

    # 5. Aplicar los cambios al modelo de la base de datos
    for key, value in update_data.items():
        setattr(db_company, key, value)

    db.commit()
    db.refresh(db_company)

    return db_company

@router.get("", response_model=List[CompanyResponse])
@cache(expire=300) 
async def read_all_companies(
    skip: int = 0,
    limit: int = Query(default=100, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get a list of all companies.
    TODO: Add permission checks (e.g., superuser only).
    """
    companies = company_crud.get_all_companies(db=db, skip=skip, limit=limit)
    return companies

@router.get("/{company_id}", response_model=CompanyResponse)
@cache(expire=300) 
async def read_specific_company_with_warehouses(
    company_id: int,
    main_db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Obtiene los datos de una empresa y sus sucursales.
    Para integraciones Bsale, prioriza la tabla 'bsale_office', pero recurre a 
    'warehouses' si la primera está vacía.
    """
    # 1. Obtener datos de la empresa y verificar permisos (sin cambios)
    company = company_crud.get_company_by_id(db=main_db, company_id=company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(status_code=403, detail="Not authorized to access this company's data")
    
    # 2. Obtener las sucursales con la nueva lógica de fallback
    warehouses = []
    if all([company.db_user, company.db_password, company.db_host, company.db_name]):
        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_session: Optional[Session] = None
        try:
            tenant_session = get_tenant_session(tenant_db_url)
            
            # --- LÓGICA DE FALLBACK AQUÍ ---

            # Primero, intentar obtener sucursales desde Bsale si la integración es la correcta
            if company.integration and company.integration.id == 1:
                warehouses = tenant_session.query(Warehouse).all()
            # Si 'warehouses' sigue siendo una lista vacía (porque no es Bsale, o porque 
            # la tabla bsale_office está vacía), entonces consultamos la tabla estándar.
            if not warehouses:
                print("No se encontraron sucursales de Bsale o no es una integración Bsale. Usando 'warehouses' como fallback.")
                warehouses = tenant_session.query(Warehouse).all()
            
            # --- FIN DE LA LÓGICA ---

        except Exception as e:
            print(f"ADVERTENCIA: No se pudo conectar a la DB del inquilino para la empresa ID {company_id}. Error: {e}")
        finally:
            if tenant_session:
                tenant_session.close()
    
    # 3. Combinar y Devolver (sin cambios)
    company.warehouses = warehouses
    
    return company

@router.get("/{company_id}/users", response_model=List[UserResponse])
@cache(expire=300) 
async def get_users_for_company(
    company_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Obtiene una lista de todos los usuarios asociados a una compañía específica.
    Solo accesible para superusuarios o miembros de esa compañía.
    """
    # 1. Verificar si la compañía existe
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Compañía no encontrada.")

    # 2. Lógica de Autorización
    is_associated = any(c.id == company_id for c in current_user.companies)
    if not current_user.is_superuser and not is_associated:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes permiso para ver los usuarios de esta compañía."
        )

    # 3. Devolver los usuarios
    return company.users

@router.delete("/{company_id}", status_code=status.HTTP_202_ACCEPTED)
async def trigger_company_deletion(
    company_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """
    Inicia la eliminación completa de una compañía y su base de datos asociada.
    Esta es una operación destructiva y se ejecuta en segundo plano.
    """
    # Verificar que la compañía existe antes de lanzar la tarea
    company = db.query(Company.id).filter_by(id=company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Compañía no encontrada.")

    # Programar la tarea de fondo
    background_tasks.add_task(delete_company_and_resources, company_id=company_id)

    return {"message": "La solicitud de eliminación para la compañía ha sido recibida y se está procesando en segundo plano. Se notificará al finalizar."}