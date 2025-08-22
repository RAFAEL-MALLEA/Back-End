import boto3
import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from fastapi import HTTPException
from database.main_db import SessionLocal
from models.main_db import Company, MetricDisplayConfiguration
from models.tenant_db import MetricName, TenantBase, Warehouse
from utils.text_formatters import slugify_for_aws
from services.notification_service import create_notification_for_superusers
from botocore.exceptions import ClientError

class TenantSetupError(Exception):
    pass


def _create_default_metric_display_configs(db: Session, company_id: int):
    """
    Crea un conjunto de configuraciones de visualización por defecto para una nueva compañía.
    """
    print(f"Creando configuraciones de visualización por defecto para la compañía ID: {company_id}...")

    # Define los textos para cada métrica
    default_texts = {
        MetricName.STOCK_CERO: {"name": "Stock Cero", "title": "Productos en Stock Cero", "abbr": "S. Cero", "info": "Productos con stock disponible cero o negativo."},
        MetricName.BAJA_ROTACION: {"name": "Baja Rotación", "title": "Productos de Baja Rotación", "abbr": "B. Rotación", "info": "Productos sin movimiento de venta reciente."},
        MetricName.STOCK_CRITICO: {"name": "Stock Crítico", "title": "Productos en Stock Crítico", "abbr": "S. Crítico", "info": "Productos con baja cobertura de stock."},
        MetricName.SOBRE_STOCK: {"name": "Sobre Stock", "title": "Productos con Sobre Stock", "abbr": "Sobre S.", "info": "Productos con un inventario excesivo según su rotación."},
        MetricName.RECOMENDACION_COMPRA: {"name": "Compra Sugerida", "title": "Sugerencias de Compra", "abbr": "Comprar", "info": "Sugerencias para reponer inventario basadas en ventas."},
        MetricName.DEVOLUCIONES: {"name": "Devoluciones", "title": "Reporte de Devoluciones", "abbr": "Devol.", "info": "Productos devueltos por clientes en el período."},
        MetricName.AJUSTE_STOCK: {"name": "Ajuste de Stock", "title": "Reporte de Ajustes", "abbr": "Ajustes", "info": "Movimientos de ajuste de inventario manuales."},
        MetricName.VENTA_SIN_STOCK: {"name": "Venta Sin Stock", "title": "Reporte de Venta Sin Stock", "abbr": "Vta. s/S", "info": "Ventas realizadas cuando el stock era negativo."},
    }

    # Define los colores y otros valores comunes
    common_configs = {
        "background_color_1": "#E3F2FD", "text_color_1": "#0D47A1",
        "text_color_2": "#424242", "background_color_3": "#BBDEFB",
        "text_color_3": "#0D47A1", "background_color_4": "#42A5F5",
        "text_color_4": "#FFFFFF", "show_counter": True, "show_grid": True,
        "show_distribution": True, "show_products": True,
        "product_operation": "greater", "product_number": 3
    }

    configs_to_add = []
    for metric_enum in MetricName:
        texts = default_texts.get(metric_enum)
        if not texts:
            continue
            
        new_config = MetricDisplayConfiguration(
            company_id=company_id,
            metric_id=metric_enum,
            name=texts["name"],
            title=texts["title"],
            short_name=texts["name"],
            short_name_2=texts["abbr"],
            short_name_3=texts["abbr"],
            short_name_4=texts["abbr"],
            abbreviation=texts["abbr"],
            information_1=texts["info"],
            information_2="",
            **common_configs
        )
        configs_to_add.append(new_config)

    if configs_to_add:
        db.bulk_save_objects(configs_to_add)
        db.commit()
        print(f"Se crearon {len(configs_to_add)} configuraciones de visualización por defecto.")

def _create_rds_instance_and_tables(company_name: str) -> dict:
    """
    Crea la instancia de RDS si no existe, espera a que esté disponible, y crea las tablas.
    Utiliza nombres sanitizados y diferentes para cada recurso de AWS.
    """
    rds = boto3.client('rds', region_name='us-east-1')
    
    # --- CORRECCIÓN EN LA GENERACIÓN DE NOMBRES ---

    # 1. Crear un identificador base seguro para AWS
    safe_base_name = slugify_for_aws(company_name)
    if not safe_base_name:
        raise TenantSetupError("El nombre de la empresa resultó en un identificador vacío después de sanitizar.")
    
    # 2. Crear el identificador para la INSTANCIA (permite guiones)
    db_instance_identifier = f"{safe_base_name}-db"
    
    # 3. Crear el nombre para la BASE DE DATOS (NO permite guiones)
    #    y para el USUARIO (mejor sin guiones).
    db_name_safe_part = safe_base_name.replace('-', '')
    db_name = f"{db_name_safe_part}_db"
    master_username = f"{db_name_safe_part}_user"[:16] # Límite de 16 caracteres para usuarios de PG

    master_password = "tenantpassword" # TODO: Usar secrets.token_hex(16)
    
    # --- FIN DE LA CORRECCIÓN ---

    try:
        # --- PASO 1: VERIFICAR SI LA INSTANCIA YA EXISTE ---
        print(f"Verificando si la instancia RDS '{db_instance_identifier}' ya existe...")
        try:
            rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
            print("La instancia ya existe. Saltando la creación y procediendo a esperar.")
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'DBInstanceNotFound':
                print("La instancia no existe. Procediendo con la creación...")
                # --- PASO 2: CREAR LA INSTANCIA DE DB CON LOS NOMBRES CORRECTOS ---
                rds.create_db_instance(
                    DBInstanceIdentifier=db_instance_identifier, # ej: "librerias-bros-db"
                    DBName=db_name,                             # ej: "libreriasbros_db"
                    Engine='postgres',
                    EngineVersion='17.2',
                    DBInstanceClass='db.t3.micro',
                    AllocatedStorage=20,
                    MasterUsername=master_username,             # ej: "libreriasbros_user"
                    MasterUserPassword=master_password,
                    VpcSecurityGroupIds=['sg-02bfc7d17d768f18b'],
                    DBSubnetGroupName='subnet-clients',
                    PubliclyAccessible=True,
                    Tags=[{'Key': 'db-cliente', 'Value': company_name}]
                )
                print("Solicitud de creación de DB enviada a AWS...")
            else:
                # Si es otro tipo de error de AWS (ej. credenciales inválidas), lo relanzamos
                raise ex

        # --- PASO 3: ESPERAR A QUE LA INSTANCIA ESTÉ DISPONIBLE ---
        print("Esperando a que la instancia de DB esté disponible (esto puede tardar varios minutos)...")
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=db_instance_identifier, WaiterConfig={'Delay': 60, 'MaxAttempts': 60})
        print("Instancia de DB disponible.")

        # --- PASO 4: OBTENER DATOS DE CONEXIÓN Y CREAR TABLAS (sin cambios) ---
        db_info_response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        endpoint = db_info_response['DBInstances'][0]['Endpoint']['Address']
        
        db_connection_info = {
            "db_name": db_name, "db_host": endpoint,
            "db_user": master_username, "db_password": master_password
        }
        
        tenant_db_url = f"postgresql://{master_username}:{master_password}@{endpoint}/{db_name}"
        tenant_engine = create_engine(tenant_db_url)
        TenantBase.metadata.create_all(bind=tenant_engine)
        print(f"Tablas del inquilino creadas en '{db_name}'.")

        with sessionmaker(bind=tenant_engine)() as session:
            session.add(Warehouse(name="Casa Matriz"))
            session.commit()

        return db_connection_info

    except Exception as e:
        print(f"ERROR DURANTE LA CREACIÓN DE LA DB: {e}")
        raise TenantSetupError(f"Fallo en la creación de la instancia RDS o migración: {str(e)}")

def setup_tenant_and_notify(company_id: int, company_name: str):
    """
    Función principal que se ejecuta en segundo plano.
    Orquesta la creación de la DB y envía notificaciones de éxito o fracaso.
    """
    main_db: Session = SessionLocal()
    try:
        # Notificar que el proceso ha comenzado
        start_message = f"⏳ Iniciando creación de base de datos para la empresa '{company_name}'."
        create_notification_for_superusers(main_db, start_message, company_id)

        # 1. Crear la DB y las tablas
        db_info = _create_rds_instance_and_tables(company_name)
        
        # 2. Actualizar el registro de Company con las credenciales
        company = main_db.query(Company).filter_by(id=company_id).first()
        if company:
            company.db_name = db_info['db_name']
            company.db_host = db_info['db_host']
            company.db_user = db_info['db_user']
            company.db_password = db_info['db_password']
            main_db.commit()
            _create_default_metric_display_configs(db=main_db, company_id=company_id)
            success_message = f"✅ Creación de base de datos para '{company_name}' finalizada con éxito."
            create_notification_for_superusers(main_db, success_message, company_id)
        
    except Exception as e:
        # Captura cualquier error, incluyendo nuestro TenantSetupError
        print("--- ERROR EN TAREA DE FONDO ---")
        traceback.print_exc()
        failure_message = f"❌ ERROR: La creación de la base de datos para '{company_name}' falló. Razón: {str(e)}"
        create_notification_for_superusers(main_db, failure_message, company_id)
    
    finally:
        main_db.close() # Siempre cerrar la sesión


def _delete_rds_instance(company_name: str):
    """
    Inicia la eliminación de una instancia de DB en RDS y espera a que se complete.
    Maneja el caso en que la instancia ya no exista.
    """
    rds = boto3.client('rds', region_name='us-east-1')
    safe_identifier = slugify_for_aws(company_name)
    db_instance_identifier = f"{safe_identifier}-db"
    
    print(f"Intentando eliminar la instancia RDS: {db_instance_identifier}...")
    try:
        rds.delete_db_instance(
            DBInstanceIdentifier=db_instance_identifier,
            SkipFinalSnapshot=True
        )
        
        print("Esperando a que la instancia de DB sea eliminada...")
        waiter = rds.get_waiter('db_instance_deleted')
        waiter.wait(DBInstanceIdentifier=db_instance_identifier, WaiterConfig={'Delay': 30, 'MaxAttempts': 60})
        print(f"Instancia RDS '{db_instance_identifier}' eliminada exitosamente.")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'DBInstanceNotFoundFault':
            print(f"La instancia RDS '{db_instance_identifier}' no fue encontrada. Se asume que ya fue eliminada.")
            return True
        else:
            raise TenantSetupError(f"Error de AWS al eliminar la instancia RDS: {str(e)}")
    except Exception as e:
        raise TenantSetupError(f"Fallo inesperado al eliminar la instancia RDS: {str(e)}")


def delete_company_and_resources(company_id: int):
    """
    Función orquestadora de fondo para eliminar una compañía y todos sus recursos.
    """
    main_db: Session = SessionLocal()
    company_name = ""
    try:
        company = main_db.query(Company).filter_by(id=company_id).first()
        if not company:
            print(f"Tarea de borrado: Compañía con ID {company_id} no encontrada. No se hace nada.")
            return

        company_name = company.name
        start_message = f"⏳ Iniciando eliminación de la empresa '{company_name}' (ID: {company_id}) y sus recursos."
        create_notification_for_superusers(main_db, start_message, company_id)

        if company.db_name and company.db_host:
            _delete_rds_instance(company.name)
        else:
            print("La compañía no tiene una base de datos RDS asociada. Saltando eliminación de DB.")

        print(f"Eliminando registro de la compañía ID {company_id} de la base de datos principal...")
        main_db.delete(company)
        main_db.commit()
        print("Registro de compañía eliminado.")

        success_message = f"✅ Eliminación de la empresa '{company_name}' (ID: {company_id}) completada con éxito."
        create_notification_for_superusers(main_db, success_message, company_id)

    except Exception as e:
        print("--- ERROR EN TAREA DE FONDO (Eliminación de Compañía) ---")
        traceback.print_exc()
        if main_db: main_db.rollback()
        
        failure_message = f"❌ ERROR: La eliminación de la empresa '{company_name}' (ID: {company_id}) falló. Razón: {str(e)}"
        create_notification_for_superusers(main_db, failure_message, company_id)
    finally:
        if main_db: main_db.close()
