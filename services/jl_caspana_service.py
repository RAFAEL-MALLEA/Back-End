from decimal import Decimal
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import date
from typing import Any, Dict, List, Optional
import traceback
from sqlalchemy.dialects import postgresql

# Tus modelos y helpers
from models.main_db import Company
from models.tenant_db import Product, Warehouse, MetricAlert, MetricName
from database.dynamic import get_tenant_session
from urllib.parse import quote_plus

def get_rp_sistemas_db_session(company_id: int, main_db: Session) -> Optional[Session]:
    db_credentials = {
        'host': '200.110.147.236',
        'port': 50128,
        'user': '3ym_sa',
        'password': '3ym_12345',
        'db_name': 'factu_3ym'
    }

    try:
        driver = "ODBC Driver 18 for SQL Server"
        driver_encoded = quote_plus(driver)

        conn_str = (
            f"mssql+pyodbc://{db_credentials['user']}:{db_credentials['password']}@"
            f"{db_credentials['host']},{db_credentials['port']}/{db_credentials['db_name']}?"
            f"driver={driver_encoded}&Encrypt=yes&TrustServerCertificate=yes"
        )

        engine = create_engine(conn_str)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        return SessionLocal()
    except Exception as e:
        print(f"Error al conectar a la base de datos de RP Sistemas para la compañía {company_id}: {e}")
        return None

def _get_product_map(tenant_db: Session) -> Dict[str, Dict[str, any]]:
    """Obtiene un mapa de productos de la DB local para un mapeo rápido."""
    all_products = tenant_db.query(Product).all()
    return {p.code: {"id": p.id, "cost": p.cost or 0} for p in all_products}

def _create_metric_alerts(
    tenant_db: Session,
    alert_data: List[Dict[str, Any]],
    product_map: Dict[str, Dict[str, Any]],
    metric_name: MetricName,
    sync_date: date,
    warehouse_id: int,
    current_stock_map: Dict[str, int]
):
    """
    Toma una lista de datos de alerta (código y valor opcional) y crea los
    registros de MetricAlert correspondientes.
    """
    unique_alerts = {}
    for item in alert_data:
        code = item.get("code")
        metric_value = item.get("value")
        product_info = product_map.get(code)
        if not product_info:
            continue

        key = (
            sync_date,
            metric_name,
            product_info["id"],
            warehouse_id,
        )
        
        unique_alerts[key] = {
            "alert_date": sync_date,
            "metric_name": metric_name,
            "product_id": product_info["id"],
            "warehouse_id": warehouse_id,
            "physical_stock_at_alert": current_stock_map.get(code, 0),
            "available_stock_at_alert": current_stock_map.get(code, 0),
            "metric_value_numeric": metric_value if isinstance(metric_value, Decimal) else None,
            "metric_value_text": str(metric_value) if metric_value is not None else None,
            "details_json": {}
        }

    alerts_to_create = list(unique_alerts.values())
    if alerts_to_create:
        stmt = pg_insert(MetricAlert).values(alerts_to_create)
        stmt = stmt.on_conflict_do_update(
            index_elements=['alert_date', 'metric_name', 'product_id', 'warehouse_id'],
            set_={
                "metric_value_numeric": stmt.excluded.metric_value_numeric,
                "metric_value_text": stmt.excluded.metric_value_text,
                "details_json": stmt.excluded.details_json
            }
        )
        tenant_db.execute(stmt)
        print(f"  -> Se insertaron/ignoraron {len(alerts_to_create)} alertas para la métrica '{metric_name.value}'.")

def sync_products_from_rp_sistemas(rp_db: Session, tenant_db: Session):
    """
    Sincroniza el catálogo de productos desde ARTICULOS a nuestra tabla Product.
    Usa un enfoque optimizado para evitar múltiples queries a la base de datos local.
    """
    print("Iniciando sincronización de productos desde RP Sistemas...")
    
    # 1. Obtener todos los productos del sistema externo (RP Sistemas)
    product_query = text("""
        SELECT A.COD_ARTICULO AS Codigo, A.DESCRIP_ARTI AS Nombre, 
               A.PRECIO_UNI AS Precio, G1.DESCRIP_AGRU AS Categoria
        FROM ARTICULOS AS A
        LEFT JOIN AGRUPACIONES AS G1 ON A.AGRU_1 = G1.CODI_AGRU AND G1.NUM_AGRU = 1
        WHERE A.ACTIVO = 'S' AND A.COD_ARTICULO NOT IN ('SI', 'VC', 'VD');
    """)
    rp_products = rp_db.execute(product_query).mappings().all()
    if not rp_products:
        print("No se encontraron productos para sincronizar en RP Sistemas.")
        return

    # 2. Obtener todos los productos existentes de nuestra base de datos UNA SOLA VEZ
    print("Obteniendo productos existentes de la base de datos local para mapeo...")
    existing_products_query = tenant_db.query(Product).all()
    # Crear un diccionario para búsqueda rápida por código (O(1) en promedio)
    existing_products_map = {p.code: p for p in existing_products_query}
    print(f"{len(existing_products_map)} productos locales cargados en memoria.")

    # Contadores para el resumen
    created_count = 0
    updated_count = 0

    # 3. Iterar sobre los productos del sistema externo y sincronizar en memoria
    for rp_prod in rp_products:
        product_code = rp_prod['Codigo']
        if not product_code:
            continue # Saltar si el código de producto está vacío en el origen
        
        # Buscar el producto en nuestro mapa en memoria
        product = existing_products_map.get(product_code)
        
        if product:
            # --- PRODUCTO EXISTE: ACTUALIZAR ---
            # Comprobar si hay cambios para evitar actualizaciones innecesarias
            has_changed = False
            if product.name != rp_prod['Nombre']:
                product.name = rp_prod['Nombre']
                has_changed = True
            if product.category != rp_prod['Categoria']:
                product.category = rp_prod['Categoria']
                has_changed = True
            
            if product.cost != rp_prod['Precio']:
                product.cost = rp_prod['Precio']
                has_changed = True

            if has_changed:
                updated_count += 1
        else:
            # --- PRODUCTO NO EXISTE: CREAR ---
            new_product = Product(
                code=product_code,
                name=rp_prod['Nombre'],
                category=rp_prod['Categoria'],
                cost=rp_prod['Precio']
            )
            tenant_db.add(new_product)
            created_count += 1
    
    # 4. Guardar todos los cambios (inserts y updates) en una sola transacción
    if created_count > 0 or updated_count > 0:
        print("Guardando cambios en la base de datos local...")
        tenant_db.commit()
        print("Cambios guardados.")
    else:
        print("No se detectaron cambios, no se necesita guardar nada.")
        
    print(f"Sincronización de productos completada. Creados: {created_count}. Actualizados: {updated_count}.")

def run_jl_caspana_alert_etl(company_id: int, main_db: Session):
    """
    Tarea de ETL que extrae los estados de las métricas de RP Sistemas para el día de hoy
    y los carga en nuestra tabla MetricAlert.
    """
    print(f"--- INICIANDO ETL DE ALERTAS PARA JL CASPANA (Compañía ID: {company_id}) ---")
    
    rp_db = None
    tenant_db = None
    try:
        # --- 1. Conexiones a las Bases de Datos ---
        company = main_db.query(Company).filter_by(id=company_id).first()
        if not company:
            print(f"Error: Compañía ID {company_id} no encontrada.")
            return

        rp_db = get_rp_sistemas_db_session(company_id, main_db)
        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_db = get_tenant_session(tenant_db_url)
        
        if not rp_db or not tenant_db:
            print("Error fatal al conectar a una de las bases de datos.")
            return

        # --- 2. Preparación de Datos ---
        # Sincronizar productos primero para asegurar que todos existen en nuestra DB
        sync_products_from_rp_sistemas(rp_db, tenant_db)
        product_map = _get_product_map(tenant_db)

        # Obtener el stock actual de todos los productos desde RP Sistemas una sola vez
        stock_query = text("WITH StockTotalPorArticulo AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT A.COD_ARTICULO AS'Codigo', ISNULL(S.StockTotal, 0) AS'Stock'FROM ARTICULOS AS A LEFT JOIN StockTotalPorArticulo AS S ON A.COD_ARTICULO = S.COD_ARTICULO WHERE A.ACTIVO ='S';")
        rp_stocks_list = rp_db.execute(stock_query).mappings().all()
        current_stock_map = {item['Codigo']: int(item['Stock'] or 0) for item in rp_stocks_list}
        
        sync_date = date.today()
        warehouse_id = 1 # Sucursal única para este cliente

        # Borrar alertas existentes para hoy para esta sucursal, para empezar de cero cada día.
        print(f"Limpiando alertas existentes para la fecha {sync_date}...")
        tenant_db.query(MetricAlert).filter(
            MetricAlert.alert_date == sync_date,
            MetricAlert.warehouse_id == warehouse_id
        ).delete(synchronize_session=False)
        tenant_db.commit()

        # --- 3. Ejecutar Queries y Cargar Alertas ---
        metric_queries_codes_only = {
            MetricName.STOCK_CERO: text("SELECT A.COD_ARTICULO AS Codigo, A.DESCRIP_ARTI AS Nombre, ISNULL(ASD.CANT_STOCK, 0) AS Stock_DEP FROM ARTICULOS AS A LEFT JOIN ARTICULOS_STOCK_DEPO AS ASD ON A.COD_ARTICULO = ASD.COD_ARTICULO AND ASD.DEPO ='DEP'WHERE A.ACTIVO ='S'AND A.COD_ARTICULO NOT IN ('SI','VC','VD') AND A.sin_stock <>'S'AND A.USA_COMPO <>'S'AND ISNULL(ASD.CANT_STOCK, 0) <= 0 AND A.CANT_STOCK <= 0 AND NOT EXISTS ( SELECT 1 FROM ARTICULOS_STOCK_DEPO AS stock_cam WHERE stock_cam.COD_ARTICULO = A.COD_ARTICULO AND stock_cam.DEPO ='CAM'AND stock_cam.CANT_STOCK >= 1 );"),
            MetricName.BAJA_ROTACION: text("WITH UltimasVentas AS ( SELECT r.ARTICULO, MAX(c.FECHA) AS UltimaFechaVenta FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT WHERE c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND LEFT(c.TIPO, 1) IN ('F','C','B','S','V') GROUP BY r.ARTICULO ) SELECT A.COD_ARTICULO FROM ARTICULOS AS A INNER JOIN ARTICULOS_STOCK_DEPO AS ASD ON A.COD_ARTICULO = ASD.COD_ARTICULO LEFT JOIN UltimasVentas UV ON A.COD_ARTICULO = UV.ARTICULO WHERE A.ACTIVO ='S'AND A.COD_ARTICULO NOT IN ('SI','VC','VD') AND A.sin_stock <>'S'AND ASD.DEPO ='DEP'AND A.USA_COMPO <>'S'AND ( UV.UltimaFechaVenta < DATEADD(day, -14, GETDATE()) OR UV.UltimaFechaVenta IS NULL );"),
            MetricName.SOBRE_STOCK: text("WITH Ventas30Dias AS ( SELECT ARTICULO, SUM(TotalVendido) AS TotalVendido30Dias FROM ( SELECT r.ARTICULO, SUM(r.CANT) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT WHERE c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND LEFT(c.TIPO, 1) IN ('F') AND c.FECHA >= DATEADD(day, -30, GETDATE()) GROUP BY r.ARTICULO UNION ALL SELECT ac.COD_ARTI_COMPO AS ARTICULO, SUM(r.CANT*ac.CANTI) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS_COMPO ac ON r.ARTICULO = ac.COD_ARTICULO WHERE c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND LEFT(c.TIPO, 1) IN ('F') AND c.FECHA >= DATEADD(day, -30, GETDATE()) GROUP BY ac.COD_ARTI_COMPO ) AS VentasCombinadas GROUP BY ARTICULO ), StockTotal AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockActualTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT a.COD_ARTICULO, a.DESCRIP_ARTI, st.StockActualTotal AS'Stock Actual Total', COALESCE(v.TotalVendido30Dias, 0)*-1 AS'Ventas 30 Días'FROM ARTICULOS a INNER JOIN StockTotal st ON a.COD_ARTICULO = st.COD_ARTICULO LEFT JOIN Ventas30Dias v ON a.COD_ARTICULO = v.ARTICULO WHERE a.ACTIVO ='S'AND a.sin_stock <>'S'AND a.USA_COMPO <>'S'AND st.StockActualTotal > COALESCE(v.TotalVendido30Dias, 0)*-1;"),
            MetricName.AJUSTE_STOCK: text("SELECT r.ARTICULO AS Codigo FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS A ON r.ARTICULO = A.COD_ARTICULO WHERE c.TIPO LIKE'S%'AND c.TIPO <>'STI'AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE) AND A.USA_COMPO <>'S' AND A.sin_stock <>'S' GROUP BY r.ARTICULO;"), 
            MetricName.RECOMENDACION_COMPRA: text("WITH PromedioVentasDiarias AS ( SELECT ARTICULO, SUM(TotalVendido) / 15.0 AS PromedioVentaDiaria FROM ( SELECT r.ARTICULO, SUM(r.CANT) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT WHERE LEFT(c.TIPO, 1) IN ('F') AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND c.FECHA >= DATEADD(day, -15, GETDATE()) GROUP BY r.ARTICULO UNION ALL SELECT ac.COD_ARTI_COMPO AS ARTICULO, SUM(r.CANT*ac.CANTI) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS_COMPO ac ON r.ARTICULO = ac.COD_ARTICULO WHERE LEFT(c.TIPO, 1) IN ('F') AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND c.FECHA >= DATEADD(day, -15, GETDATE()) GROUP BY ac.COD_ARTI_COMPO ) AS VentasCombinadas GROUP BY ARTICULO ), StockTotal AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockActualTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT A.COD_ARTICULO AS Codigo FROM ARTICULOS AS A INNER JOIN StockTotal st ON A.COD_ARTICULO = st.COD_ARTICULO LEFT JOIN PromedioVentasDiarias PVD ON A.COD_ARTICULO = PVD.ARTICULO WHERE A.ACTIVO ='S'AND A.COD_ARTICULO NOT IN ('SI','VC','VD') AND A.USA_COMPO <>'S'AND COALESCE(st.StockActualTotal, 0) <= ( ISNULL(A.DIAS_DEMORA_ENTRE, 0)*COALESCE(PVD.PromedioVentaDiaria, 0) )"),
            MetricName.DEVOLUCIONES: text("SELECT DISTINCT r.ARTICULO FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS A ON r.ARTICULO = A.COD_ARTICULO WHERE LEFT(c.TIPO, 1) ='C'AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE) AND A.USA_COMPO <>'S' AND A.sin_stock <>'S';"),
            MetricName.VENTA_SIN_STOCK: text("WITH VentasComponentesHoy AS ( SELECT CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN ac.COD_ARTI_COMPO ELSE r.ARTICULO END AS ArticuloComponente, SUM( CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN r.CANT*ac.CANTI ELSE r.CANT END ) AS TotalVendidoHoy FROM COMP_EMITIDOS AS c INNER JOIN RENG_FAC AS r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT LEFT JOIN ARTICULOS_COMPO AS ac ON r.ARTICULO = ac.COD_ARTICULO WHERE CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE) AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND LEFT(c.TIPO, 1) ='F'GROUP BY CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN ac.COD_ARTI_COMPO ELSE r.ARTICULO END ), StockTotal AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockActualTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT Ventas.ArticuloComponente AS Codigo, a.DESCRIP_ARTI as nombre FROM VentasComponentesHoy AS Ventas INNER JOIN ARTICULOS AS a ON Ventas.ArticuloComponente = a.COD_ARTICULO LEFT JOIN StockTotal AS st ON Ventas.ArticuloComponente = st.COD_ARTICULO WHERE ISNULL(st.StockActualTotal, 0) <= 0 AND a.CANT_STOCK <= 0 AND a.SIN_STOCK <>'S';"),
            MetricName.STOCK_CRITICO: text("WITH Ventas30Dias AS ( SELECT ARTICULO, SUM(TotalVendido) AS TotalVendido30Dias FROM ( SELECT r.ARTICULO, SUM(r.CANT) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT WHERE LEFT(c.TIPO, 1) IN ('F') AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND c.FECHA >= DATEADD(day, -30, GETDATE()) GROUP BY r.ARTICULO UNION ALL SELECT ac.COD_ARTI_COMPO AS ARTICULO, SUM(r.CANT*ac.CANTI) AS TotalVendido FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS_COMPO ac ON r.ARTICULO = ac.COD_ARTICULO WHERE LEFT(c.TIPO, 1) IN ('F') AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND c.FECHA >= DATEADD(day, -30, GETDATE()) GROUP BY ac.COD_ARTI_COMPO ) AS VentasCombinadas GROUP BY ARTICULO ), StockTotal AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockActualTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT a.COD_ARTICULO AS Codigo FROM ARTICULOS a INNER JOIN StockTotal st ON a.COD_ARTICULO = st.COD_ARTICULO INNER JOIN Ventas30Dias v ON a.COD_ARTICULO = v.ARTICULO WHERE a.ACTIVO ='S'AND a.COD_ARTICULO NOT IN ('SI','VC','VD') AND a.USA_COMPO <>'S'AND st.StockActualTotal <= v.TotalVendido30Dias*-1 AND v.TotalVendido30Dias*-1 > 0 AND a.SIN_STOCK <>'S';")
        }

        metric_queries_with_value = {
            MetricName.AJUSTE_STOCK: text("""
            SELECT
                r.ARTICULO AS Codigo,
                SUM(r.CANT) AS Cantidad
            FROM
                COMP_EMITIDOS c
            INNER JOIN
                RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT
            INNER JOIN
                ARTICULOS A ON r.ARTICULO = A.COD_ARTICULO
            WHERE
                c.TIPO LIKE 'S%' 
                AND c.TIPO <> 'STI'
                AND c.ESTADO != 'ANU'
                AND c.ACTUA_STOCK = 'S'
                AND CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE)
                AND A.USA_COMPO <> 'S'
            GROUP BY
                r.ARTICULO;
            """),
            MetricName.DEVOLUCIONES: text("""
                SELECT
                    r.ARTICULO AS Codigo,
                    SUM(r.CANT) AS Cantidad
                FROM
                    COMP_EMITIDOS c
                INNER JOIN
                    RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT
                INNER JOIN
                    ARTICULOS A ON r.ARTICULO = A.COD_ARTICULO
                WHERE
                    LEFT(c.TIPO, 1) = 'C'
                    AND c.ESTADO != 'ANU'
                    AND c.ACTUA_STOCK = 'S'
                    AND CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE)
                    AND A.USA_COMPO <> 'S'
                GROUP BY
                    r.ARTICULO;
            """),
            MetricName.VENTA_SIN_STOCK: text("WITH VentasComponentesHoy AS ( SELECT CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN ac.COD_ARTI_COMPO ELSE r.ARTICULO END AS ArticuloComponente, SUM( CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN r.CANT*ac.CANTI ELSE r.CANT END ) AS TotalVendidoHoy FROM COMP_EMITIDOS AS c INNER JOIN RENG_FAC AS r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT LEFT JOIN ARTICULOS_COMPO AS ac ON r.ARTICULO = ac.COD_ARTICULO WHERE CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE) AND c.ESTADO !='ANU'AND c.ACTUA_STOCK ='S'AND LEFT(c.TIPO, 1) ='F'GROUP BY CASE WHEN ac.COD_ARTICULO IS NOT NULL THEN ac.COD_ARTI_COMPO ELSE r.ARTICULO END ), StockTotal AS ( SELECT COD_ARTICULO, SUM(CANT_STOCK) AS StockActualTotal FROM ARTICULOS_STOCK_DEPO WHERE DEPO ='DEP'GROUP BY COD_ARTICULO ) SELECT Ventas.ArticuloComponente AS Codigo, ISNULL(st.StockActualTotal, 0) as Cantidad FROM VentasComponentesHoy AS Ventas INNER JOIN ARTICULOS AS a ON Ventas.ArticuloComponente = a.COD_ARTICULO LEFT JOIN StockTotal AS st ON Ventas.ArticuloComponente = st.COD_ARTICULO WHERE ISNULL(st.StockActualTotal, 0) <= 0 AND a.CANT_STOCK <= 0 AND a.SIN_STOCK <>'S';"),
            MetricName.RECOMENDACION_COMPRA: text("""
                WITH
                PromedioVentasDiarias AS (
                    SELECT
                    ARTICULO,
                    SUM(TotalVendido) / 15.0 AS PromedioVentaDiaria
                    FROM
                    (
                        SELECT
                        r.ARTICULO,
                        SUM(r.CANT) AS TotalVendido
                        FROM
                        COMP_EMITIDOS c
                        INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT
                        AND c.NUM = r.NUM_FACT
                        WHERE
                        LEFT(c.TIPO, 1) IN ('F')
                        AND c.ESTADO != 'ANU'
                        AND c.ACTUA_STOCK = 'S'
                        AND c.FECHA >= DATEADD(day, -15, GETDATE())
                        GROUP BY
                        r.ARTICULO
                        UNION ALL
                        SELECT
                        ac.COD_ARTI_COMPO AS ARTICULO,
                        SUM(r.CANT * ac.CANTI) AS TotalVendido
                        FROM
                        COMP_EMITIDOS c
                        INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT
                        AND c.NUM = r.NUM_FACT
                        INNER JOIN ARTICULOS_COMPO ac ON r.ARTICULO = ac.COD_ARTICULO
                        WHERE
                        LEFT(c.TIPO, 1) IN ('F')
                        AND c.ESTADO != 'ANU'
                        AND c.ACTUA_STOCK = 'S'
                        AND c.FECHA >= DATEADD(day, -15, GETDATE())
                        GROUP BY
                        ac.COD_ARTI_COMPO
                    ) AS VentasCombinadas
                    GROUP BY
                    ARTICULO
                ),
                StockTotal AS (
                    SELECT
                    COD_ARTICULO,
                    SUM(CANT_STOCK) AS StockActualTotal
                    FROM
                    ARTICULOS_STOCK_DEPO WHERE DEPO = 'DEP'
                    GROUP BY
                    COD_ARTICULO
                )
                SELECT
                A.COD_ARTICULO AS Codigo,
                (
                    (
                    ISNULL(A.DIAS_DEMORA_ENTRE, 0) * COALESCE(PVD.PromedioVentaDiaria, 0) * -1
                    ) - COALESCE(st.StockActualTotal, 0)
                ) AS Cantidad
                FROM
                ARTICULOS AS A
                INNER JOIN StockTotal st ON A.COD_ARTICULO = st.COD_ARTICULO
                LEFT JOIN PromedioVentasDiarias PVD ON A.COD_ARTICULO = PVD.ARTICULO
                WHERE
                A.ACTIVO = 'S'
                AND A.COD_ARTICULO NOT IN ('SI', 'VC', 'VD')
                AND A.USA_COMPO <> 'S'
                AND COALESCE(st.StockActualTotal, 0) <= (
                    ISNULL(A.DIAS_DEMORA_ENTRE, 0) * COALESCE(PVD.PromedioVentaDiaria, 0)
                )
            """),
        }
        # --- 3. Ejecutar Queries y Cargar Alertas ---

        # Procesar métricas que solo devuelven códigos
        for metric_name, query in metric_queries_codes_only.items():
            print(f"Ejecutando query para la métrica: {metric_name.value}...")
            # Mapear el resultado a la estructura que espera el helper
            alert_data = [{"code": row[0]} for row in rp_db.execute(query).fetchall()]
            _create_metric_alerts(
                tenant_db, alert_data, product_map, metric_name, 
                sync_date, warehouse_id, current_stock_map
            )

        # Procesar métricas que devuelven código y valor
        for metric_name, query in metric_queries_with_value.items():
            print(f"Ejecutando query con valor para la métrica: {metric_name.value}...")
            # Mapear el resultado (que ahora tiene 'Codigo' y 'Cantidad') a la estructura del helper
            results_with_value = rp_db.execute(query).mappings().all()
            alert_data = [{"code": row['Codigo'], "value": row['Cantidad']} for row in results_with_value]
            _create_metric_alerts(
                tenant_db, alert_data, product_map, metric_name, 
                sync_date, warehouse_id, current_stock_map
            )

        # Guardar todos los nuevos registros de alerta
        tenant_db.commit()
        print("--- ETL DE ALERTAS PARA JL CASPANA FINALIZADO EXITOSAMENTE ---")

    except Exception as e:
        if tenant_db: tenant_db.rollback()
        print(f"--- ERROR DURANTE EL ETL DE ALERTAS PARA JL CASPANA ---")
        traceback.print_exc()
    finally:
        if rp_db: rp_db.close()
        if tenant_db: tenant_db.close()