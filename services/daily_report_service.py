import pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from collections import defaultdict
from datetime import date
from typing import List, Dict, Any, Optional
from decimal import Decimal
import traceback
from openpyxl.styles import Font
# --- Dependencias y Modelos ---
from models.main_db import Company
from models.tenant_db import MetricAlert, Product, Warehouse, MetricName
from database.dynamic import get_tenant_session
from services.email_service import send_email_with_attachments
from services.jl_caspana_service import get_rp_sistemas_db_session
from utils.calculate_alerts_days import get_consecutive_alert_days

def _create_excel_in_memory(
    main_data: List[Dict[str, Any]], 
    summary_data: List[Dict[str, Any]],
    metric_total_value: Decimal,
    metric_name: str
) -> BytesIO:
    """
    Toma datos, un resumen, y un total, y devuelve un archivo Excel en memoria
    con formato profesional.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # --- Escribir el Resumen de Top 5 Productos ---
        # Convertir los datos del resumen a un DataFrame de pandas
        df_summary = pd.DataFrame(summary_data)
        # Escribir el DataFrame en la hoja de Excel, empezando en la primera fila
        df_summary.to_excel(writer, index=False, sheet_name='Reporte', startrow=0)
        
        # --- Escribir el Total de la Métrica ---
        # Crear un DataFrame de una sola fila para el total y escribirlo
        total_df = pd.DataFrame([{" ": "Valor Total de la Métrica:", " ": f"${metric_total_value:,.2f}"}])
        total_df.to_excel(writer, index=False, sheet_name='Reporte', startrow=len(df_summary) + 2, header=False)

        # --- Escribir la Tabla Principal de Datos ---
        # Dejar un par de filas en blanco y luego escribir la tabla principal
        df_main = pd.DataFrame(main_data)
        df_main.to_excel(writer, index=False, sheet_name='Reporte', startrow=len(df_summary) + 5)
        
        # --- Aplicar Estilos y Auto-ajustar Columnas ---
        workbook = writer.book
        worksheet = writer.sheets['Reporte']
        
        # Estilo para cabeceras (Negrita)
        header_font = Font(name='Calibri', size=11, bold=True)

        # Aplicar estilo a las cabeceras del resumen (fila 1)
        for col in worksheet.iter_cols(min_row=1, max_row=1, min_col=1, max_col=len(df_summary.columns)):
            for cell in col:
                cell.font = header_font
        # Aplicar estilo a las cabeceras de la tabla principal
        main_table_header_row = len(df_summary) + 6
        for col in worksheet.iter_cols(min_row=main_table_header_row, max_row=main_table_header_row, min_col=1, max_col=len(df_main.columns)):
            for cell in col:
                cell.font = header_font
        
        # Auto-ajustar el ancho de las columnas
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells if cell.value)
            worksheet.column_dimensions[column_cells[0].column_letter].width = length + 2

    output.seek(0)
    return output

def generate_and_email_daily_report(company_id: int, main_db_session: Session):
    """
    Función de fondo que genera y envía un reporte diario por correo,
    separado por sucursal, con resúmenes en el cuerpo y en los archivos Excel.
    """
    print(f"Iniciando generación de reporte diario para la compañía ID: {company_id}")
    tenant_session: Optional[Session] = None
    try:
        company = main_db_session.query(Company).filter_by(id=company_id).first()
        if not company or not all([company.db_user, company.db_password, company.db_host, company.db_name]):
            print(f"Error: Compañía {company_id} no encontrada o con configuración de DB incompleta.")
            return

        tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
        tenant_session = get_tenant_session(tenant_db_url)
        
        warehouses = tenant_session.query(Warehouse).all()
        today = date.today()

        for warehouse in warehouses:
            attachments = []
            grand_total_value_in_alerts = Decimal(0)
            email_summary_rows = ""

            print(f"Procesando sucursal: {warehouse.name} (ID: {warehouse.id})")
            if company.id == 47:
                print(f"Ejecutando reportes personalizados para JL Caspana (ID: 47)...")
                try:
                    rp_db = get_rp_sistemas_db_session(company_id, main_db_session)
                    if rp_db:
                        # --- Reporte 1: Ingresos de Hoy ---
                        print("  -> Generando reporte: Ingresos de Hoy")
                        ingresos_query = text("""
                            SELECT r.ARTICULO AS Codigo, a.DESCRIP_ARTI AS Nombre,
                            SUM(r.CANT) AS Cantidad_Recibida_hoy FROM COMP_EMITIDOS c INNER JOIN RENG_FAC r ON c.TIPO = r.TIPO_FACT
                            AND c.NUM = r.NUM_FACT INNER JOIN ARTICULOS a ON r.ARTICULO = a.COD_ARTICULO
                            WHERE c.TIPO = 'STI' AND c.ESTADO != 'ANU' AND CAST(c.FECHA AS DATE) = CAST(GETDATE() AS DATE) AND a.SIN_STOCK = 'N' AND c.TIPO_MOVI = 'I' AND a.COD_ARTICULO != '5206'
                            GROUP BY r.ARTICULO, a.DESCRIP_ARTI ORDER BY a.DESCRIP_ARTI
                        """)
                        ingresos_data = [dict(row) for row in rp_db.execute(ingresos_query).mappings().all()]
                        if ingresos_data:
                            total_ingresos = sum(item['Cantidad_Recibida_hoy'] for item in ingresos_data)
                            excel_file = _create_excel_in_memory(ingresos_data, [], Decimal(0), "Ingresos de Hoy")
                            attachments.append({"filename": f"Ingresos_Hoy_{warehouse.name}_{today}.xlsx", "data": excel_file})
                            email_summary_rows += f"<tr><td style='padding: 8px; border-bottom: 1px solid #ddd;'>📈 Ingresos de Hoy</td><td style='padding: 8px; border-bottom: 1px solid #ddd; text-align: right;'>{total_ingresos} unidades</td></tr>"

                        # --- Reporte 2: Venta Bajo Costo ---
                        print("  -> Generando reporte: Venta Bajo Costo")
                        bajo_costo_query = text("""
                        SELECT
                            a.COD_ARTICULO,
                            a.DESCRIP_ARTI,
                            a.COSTO_UNI_SIN_DTO,
                            b.PRECIO_VTA AS PRECIO_VENTA_LISTA,
                            b.PRECIO_VTA_OFER AS PRECIO_OFERTA,
                            b.LISTA_CODI
                        FROM
                            ARTICULOS a
                        JOIN
                            LISTAS_ITEMS b ON a.COD_ARTICULO = b.ARTICULO
                        WHERE
                            a.ACTIVO = 'S'
                            AND b.LISTA_CODI = '01'
                            AND (
                                b.PRECIO_VTA < a.COSTO_UNI_SIN_DTO
                                OR
                                (
                                    b.PRECIO_VTA_OFER IS NOT NULL
                                    AND b.PRECIO_VTA_OFER < a.COSTO_UNI_SIN_DTO
                                    AND (b.FECHA_HASTA_OFER IS NULL OR b.FECHA_HASTA_OFER >= CAST(GETDATE() AS DATE))
                                )
                        );
                        """)
                        bajo_costo_data = [dict(row) for row in rp_db.execute(bajo_costo_query).mappings().all()]
                        if bajo_costo_data:
                            excel_file = _create_excel_in_memory(bajo_costo_data, [],  Decimal(0), "Venta Bajo Costo")
                            attachments.append({"filename": f"Venta_Bajo_Costo_{warehouse.name}_{today}.xlsx", "data": excel_file})
                            email_summary_rows += f"<tr><td style='padding: 8px; border-bottom: 1px solid #ddd;'>📉 Venta Bajo Costo</td><td style='padding: 8px; border-bottom: 1px solid #ddd; text-align: right;'>{len(bajo_costo_data)} productos</td></tr>"
                        # --- Reporte 3: Actualización de Precios y Stock ---
                        print("  -> Generando reporte: Actualización de Precios y Stock")
                        precios_query = text("""
                            select
                                a.COD_ARTICULO, a.DESCRIP_ARTI, a.UM, a.PUNTO_REPO, 
                                a.STOCK_MIN, a.STOCK_MAX, a.USA_COMPO, a.SIN_STOCK, 
                                a.ACTIVO, a.CLASIFIC_ABC, a.FECHA_ULTIMO_MOV, a.UNI_X_BULTO,
                                a.WEB_PUBLI, a.COSTO_UNI_SIN_DTO, a.AGRU_1,
                                b.PRECIO_VTA, b.FECHA_MODI,
                                c.DEPO, c.CANT_STOCK
                            from 
                                ARTICULOS a
                            join 
                                LISTAS_ITEMS b on a.COD_ARTICULO = b.ARTICULO
                            join
                                ARTICULOS_STOCK_DEPO c ON a.COD_ARTICULO=c.COD_ARTICULO
                        """)
                        precios_data = [dict(row) for row in rp_db.execute(precios_query).mappings().all()]
                        if precios_data:
                            excel_file = _create_excel_in_memory(precios_data, [], Decimal(0), "Actualización de Precios")
                            attachments.append({"filename": f"Precios_Y_Stock_{warehouse.name}_{today}.xlsx", "data": excel_file})
                            email_summary_rows += f"<tr><td style='padding: 8px; border-bottom: 1px solid #ddd;'>📋 Reporte de Precios/Stock</td><td style='padding: 8px; border-bottom: 1px solid #ddd; text-align: right;'>{len(precios_data)} productos</td></tr>"
                            
                            # --- Reporte 4: Productos Clasificados por ABC ---
                            print("  -> Generando reporte: Productos Clasificados por ABC")
                            abc_query = text("""
/***************************************************************************************************
*
*
* LÓGICA:
* 1.  CTE 'StockTotalPorArticulo': Obtiene el stock actual real desde la tabla de depósitos.
* 2.  CTE 'VentasAgrupadasPorComponente': Calcula el total de unidades VENDIDAS en los
* últimos 15 días, manejando productos compuestos.
* 3.  CTE 'IngresosAgrupadosPorComponente': Calcula el total de unidades INGRESADAS
* en los últimos 15 días (TIPO = 'STI').
* 4.  Consulta Principal:
* - Une los datos de artículos, stock, ventas e ingresos.
* - Calcula el stock inicial del período.
* - Calcula la rotación usando la fórmula: (Ventas / Stock Inicial) * 100.
*
***************************************************************************************************/

-- CTE para obtener el stock total actual por artículo desde el depósito
WITH StockTotalPorArticulo AS (
    SELECT
        COD_ARTICULO,
        SUM(CANT_STOCK) AS StockTotal
    FROM
        ARTICULOS_STOCK_DEPO
    WHERE DEPO = 'DEP'
    GROUP BY
        COD_ARTICULO
    
),

-- CTE para desglosar y sumar las VENTAS de los últimos 15 días
VentasAgrupadasPorComponente AS (
    SELECT
        COALESCE(AC.COD_ARTI_COMPO, R.ARTICULO) AS ArticuloComponente,
        SUM(R.CANT * COALESCE(AC.CANTI, 1)*-1) AS TotalVendido15Dias
    FROM
        RENG_FAC AS R
    INNER JOIN
        COMP_EMITIDOS AS C ON R.TIPO_FACT = C.TIPO AND R.NUM_FACT = C.NUM
    LEFT JOIN
        ARTICULOS_COMPO AS AC ON R.ARTICULO = AC.COD_ARTICULO
    WHERE
        C.FECHA >= DATEADD(DAY, -15, GETDATE())
        AND C.ESTADO != 'ANU'
        AND C.ACTUA_STOCK = 'S'
        AND LEFT(C.TIPO, 1) IN ('F') -- Considerando solo Facturas como Venta
    GROUP BY
        COALESCE(AC.COD_ARTI_COMPO, R.ARTICULO)
),

-- CTE para sumar los INGRESOS de productos de los últimos 15 días
IngresosAgrupadosPorComponente AS (
    SELECT
        r.ARTICULO AS ArticuloComponente, -- Asumiendo que los ingresos son siempre de componentes base
        SUM(r.CANT) AS TotalIngresado15Dias
    FROM
        COMP_EMITIDOS c
    INNER JOIN
        RENG_FAC r ON c.TIPO = r.TIPO_FACT AND c.NUM = r.NUM_FACT
    WHERE
        c.TIPO = 'STI' -- Tipo de comprobante de Ingreso de Stock
        AND c.ESTADO != 'ANU'
        AND c.FECHA >= DATEADD(DAY, -15, GETDATE())
    GROUP BY
        r.ARTICULO
)

-- Consulta final para calcular la rotación
SELECT
    A.COD_ARTICULO AS CodigoProducto,
    A.DESCRIP_ARTI AS DescripcionProducto,
    -- % Rotación
    CASE
        WHEN (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0)) <= 0 THEN 0
        ELSE (COALESCE(V.TotalVendido15Dias,0) * 100.0) / (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0))
    END AS Rotacion,
    -- Clasificación ABC
    CASE
        WHEN 
            CASE
                WHEN (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0)) <= 0 THEN 0
                ELSE (COALESCE(V.TotalVendido15Dias,0) * 100.0) / (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0))
            END > 60 THEN 'A'
        WHEN 
            CASE
                WHEN (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0)) <= 0 THEN 0
                ELSE (COALESCE(V.TotalVendido15Dias,0) * 100.0) / (COALESCE(S.StockTotal,0) + COALESCE(V.TotalVendido15Dias,0) - COALESCE(I.TotalIngresado15Dias,0))
            END BETWEEN 20 AND 60 THEN 'B'
        ELSE 'C'
    END AS Clasificacion_abc
FROM
    ARTICULOS AS A
LEFT JOIN StockTotalPorArticulo S ON A.COD_ARTICULO = S.COD_ARTICULO
LEFT JOIN VentasAgrupadasPorComponente V ON A.COD_ARTICULO = V.ArticuloComponente
LEFT JOIN IngresosAgrupadosPorComponente I ON A.COD_ARTICULO = I.ArticuloComponente
WHERE
    A.USA_COMPO  = 'N'
    AND A.ACTIVO = 'S'
ORDER BY
    Rotacion DESC;
""")
                            abc_data = [dict(row) for row in rp_db.execute(abc_query).mappings().all()]
                            if abc_data:
                                excel_file = _create_excel_in_memory(abc_data, [], Decimal(0), "Productos Clasificados por ABC")
                                attachments.append({"filename": f"Productos_Clasificados_ABC_{warehouse.name}_{today}.xlsx", "data": excel_file})
                                email_summary_rows += f"<tr><td style='padding: 8px; border-bottom: 1px solid #ddd;'>🔤 Productos Clasificados por ABC</td><td style='padding: 8px; border-bottom: 1px solid #ddd; text-align: right;'>{len(abc_data)} productos</td></tr>"
                except Exception as e_custom:
                    print(f"ERROR al generar reportes personalizados para JL Caspana: {e_custom}")
                finally:
                    if rp_db:
                        rp_db.close()
            metrics_to_report = [
                MetricName.STOCK_CERO, MetricName.BAJA_ROTACION, MetricName.STOCK_CRITICO,
                MetricName.SOBRE_STOCK, MetricName.RECOMENDACION_COMPRA, MetricName.DEVOLUCIONES,
                MetricName.AJUSTE_STOCK, MetricName.VENTA_SIN_STOCK
            ]
            
            alerts_query = tenant_session.query(MetricAlert, Product).join(Product)\
                .filter(MetricAlert.alert_date == today, MetricAlert.warehouse_id == warehouse.id)\
                .filter(MetricAlert.metric_name.in_(metrics_to_report))
                
            all_alerts_today = alerts_query.all()

            if not all_alerts_today:
                print(f"No hay alertas para reportar en la sucursal {warehouse.name}. Saltando.")
                continue
            
            alerts_by_metric: Dict[MetricName, List] = defaultdict(list)
            for alert, product in all_alerts_today:
                alerts_by_metric[alert.metric_name].append((alert, product))
            
            for metric_name, alerts in alerts_by_metric.items():
                excel_data = []
                metric_specific_total_value = Decimal(0)

                for alert, product in alerts:
                    cost = product.cost or Decimal(0)
                    units = 0
                    
                    if metric_name in [MetricName.STOCK_CRITICO, MetricName.BAJA_ROTACION, MetricName.SOBRE_STOCK]:
                        units = alert.available_stock_at_alert or 0
                    elif metric_name == MetricName.VENTA_SIN_STOCK:
                        units = abs(alert.available_stock_at_alert or 0)
                    elif metric_name in [MetricName.RECOMENDACION_COMPRA, MetricName.DEVOLUCIONES, MetricName.AJUSTE_STOCK]:
                        units = int(Decimal(alert.metric_value_numeric or 0))
                    
                    item_value = Decimal(units) * cost
                    metric_specific_total_value += item_value

                    consecutive_days = get_consecutive_alert_days(
                        db=tenant_session,
                        product_id=product.id,
                        warehouse_id=warehouse.id,
                        metric_name=alert.metric_name,
                        end_date=alert.alert_date
                    )
                    
                    excel_row = {
                        "Producto": product.name,
                        "Codigo": product.code,
                        "Dias en Alerta": consecutive_days,
                        "Costo Unitario": float(cost),
                        "Unidades Afectadas": units,
                        "Valor Afectado ($)": float(item_value)
                    }
                    
                    if metric_name == MetricName.RECOMENDACION_COMPRA:
                        excel_row["Unidades a Comprar"] = units # Ya es la unidad a comprar

                    excel_data.append(excel_row)
                
                grand_total_value_in_alerts += metric_specific_total_value
                
                excel_data.sort(key=lambda x: x["Valor Afectado ($)"], reverse=True)
                summary_for_excel = [
                    {"Top 5 Productos por Valor": row["Producto"], "Valor Afectado": f"${row['Valor Afectado ($)']:,.2f}"}
                    for row in excel_data[:5]
                ]

                metric_title = metric_name.name.replace("_", " ").title()
                excel_file = _create_excel_in_memory(
                    main_data=excel_data,
                    summary_data=summary_for_excel,
                    metric_total_value=metric_specific_total_value,
                    metric_name=metric_title
                )
                attachments.append({
                    "filename": f"{metric_name.value.upper()}_{warehouse.name.replace(' ', '_')}_{today}.xlsx",
                    "data": excel_file
                })

                email_summary_rows += f"<tr><td style='padding: 8px; border-bottom: 1px solid #ddd;'>{metric_title}</td><td style='padding: 8px; border-bottom: 1px solid #ddd; text-align: right;'>${metric_specific_total_value:,.2f}</td></tr>"

            if attachments:
                subject = f"📊 Reporte Diario de Alertas: {company.name} / {warehouse.name} - {today.strftime('%d-%m-%Y')}"
                body_html = f"""
                <html><head><style>body {{font-family: Arial, sans-serif; color: #333;}} table {{width: 100%; max-width: 600px; border-collapse: collapse; margin-top: 20px;}} th, td {{padding: 10px; text-align: left; border-bottom: 1px solid #ddd;}} th {{ background-color: #f2f2f2; }}</style></head>
                <body>
                    <h1>📊 Resumen de Alertas Diarias</h1>
                    <p>Hola, este es el reporte automático para <strong>{company.name} (Sucursal: {warehouse.name})</strong> del día <strong>{today.strftime('%d de %B, %Y')}</strong>.</p>
                    <h2>💰 Valor Total en Alertas: ${grand_total_value_in_alerts:,.2f}</h2>
                    <p>A continuación, el desglose por tipo de métrica:</p>
                    <table>
                        <thead><tr><th>Métrica</th><th style="text-align: right;">Valor Afectado</th></tr></thead>
                        <tbody>{email_summary_rows}</tbody>
                    </table>
                    <p>Se han adjuntado los reportes detallados para cada tipo de métrica en formato Excel. Cada archivo incluye un resumen con los 5 productos de mayor impacto.</p>
                    <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                    <p style="font-size: 0.8em; color: #777;"><em>Este es un correo generado automáticamente por Inventaria.</em></p>
                </body></html>
                """
                recipients = {user.email for user in company.users if user.email}
                recipients.add("jairojairjason@gmail.com")
                recipients.add("slewinr@quot.cl")
                # Usalo para pruebas
                #recipients = ["jairojairjason@gmail.com"]

                send_email_with_attachments(
                    to_addresses=list(recipients),
                    subject=subject, body_html=body_html, attachments=attachments
                )
    
    except Exception as e:
        print(f"--- ERROR en la tarea de reporte diario para compañía {company_id} ---")
        traceback.print_exc()
    finally:
        if tenant_session: tenant_session.close()