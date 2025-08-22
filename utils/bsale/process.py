from decimal import Decimal
import re
from urllib.parse import parse_qs, urlparse
from models.bsale_db import Bsale_Coin, Bsale_Document, Bsale_Document_Detail, Bsale_Document_Tax, Bsale_Document_Type, Bsale_Office, Bsale_Price_List, Bsale_Price_List_Detail, Bsale_Product, Bsale_Product_Type, Bsale_Seller, Bsale_Stock, Bsale_Tax, Bsale_User, Bsale_Variant
from utils.bsale.fetch_api import get_bsale
from database.dynamic import get_tenant_session
from datetime import datetime, timezone

def get_or_create_document_type(doc_type,tenant_session):
    doc_id = doc_type.get("id")
    doc_name = doc_type.get("name")

    doc_type = tenant_session.query(Bsale_Document_Type).filter_by(id=doc_id).first()
    if doc_type:
        return doc_type
    else:
        new_doc_type = Bsale_Document_Type(id=doc_id, name=doc_name)
        tenant_session.add(new_doc_type)
        tenant_session.commit()
        return new_doc_type

def get_or_create_bsale_coin(tenant_session, coin_data: dict,api_key):
    """
    Obtiene una moneda de Bsale por su ID o la crea si no existe.

    Args:
        tenant_session: Sesión de SQLAlchemy.
        coin_data: Diccionario con la información de la moneda.

    Returns:
        El objeto Bsale_Coin existente o recién creado.
    """
    coin_id = coin_data.get('id')
    bsale_coin = tenant_session.query(Bsale_Coin).filter_by(id=coin_id).first()
    if bsale_coin:
        return bsale_coin
    else:
        print(coin_data)
        coin = get_bsale(api_key, endpoint=f"/v1/coins/{coin_id}.json")[0]

        bsale_coin = Bsale_Coin(
            id=coin.get('id'),
            name=coin.get('name'),
            symbol=coin.get('symbol'),
            decimals=coin.get('decimals'),
            round_decimals=coin.get('roundDecimals'),
            total_round=bool(coin.get('totalRound', 0)),
            name_in_words=coin.get('nameInWords'),
            iso_code=coin.get('isoCode')
        )
        tenant_session.add(bsale_coin)
        tenant_session.commit()
        return bsale_coin

def get_or_create_bsale_product_type(tenant_session, product_type_data: dict,api_key):
    """
    Obtiene un tipo de producto de Bsale por su ID o lo crea si no existe.

    Args:
        tenant_session: Sesión de SQLAlchemy.
        product_type_data: Diccionario con la información del tipo de producto.

    Returns:
        El objeto Bsale_Product_Type existente o recién creado.
    """
    product_type_id = product_type_data.get('id')
    bsale_product_type = tenant_session.query(Bsale_Product_Type).filter_by(id=product_type_id).first()
    if bsale_product_type:
        return bsale_product_type
    else:
        product_type_data = get_bsale(api_key, endpoint=f"/v1/product_types/{product_type_id}.json")[0]
        bsale_product_type = Bsale_Product_Type(
            id=product_type_data.get('id'),
            name=product_type_data.get('name'),
            is_editable=bool(product_type_data.get('isEditable', 0)),
            state=bool(product_type_data.get('state', 0)),
            imagestion_category_id=product_type_data.get('imagestionCategoryId', 0),
            prestashop_category_id=product_type_data.get('prestashopCategoryId', 0)
        )
        tenant_session.add(bsale_product_type)
        tenant_session.commit()
        return bsale_product_type

def get_or_create_bsale_product(tenant_session, api_key: str, product_id: int):
    """
    Obtiene un producto de Bsale por su ID o lo crea si no existe,
    incluyendo la creación/obtención del tipo de producto asociado.

    Args:
        tenant_session: Sesión de SQLAlchemy.
        api_key: Clave de la API de Bsale.
        product_id: ID del producto.

    Returns:
        El objeto Bsale_Product existente o recién creado.
    """
    bsale_product = tenant_session.query(Bsale_Product).filter_by(id=product_id).first()
    if bsale_product:
        return bsale_product
    else:
        product_data = get_bsale(api_key, endpoint=f"/v1/products/{product_id}.json")[0]
        if product_data:
            product_type_data = product_data.get('product_type')
            bsale_product_type = get_or_create_bsale_product_type(tenant_session, {'id': product_type_data.get('id')},api_key) if product_type_data else None

            if bsale_product_type:
                bsale_product = Bsale_Product(
                    id=int(product_data.get('id')),
                    name=product_data.get('name'),
                    description=product_data.get('description'),
                    classification=product_data.get('classification', 0),
                    ledger_account=product_data.get('ledgerAccount'),
                    cost_center=product_data.get('costCenter'),
                    allow_decimal=bool(product_data.get('allowDecimal', 0)),
                    stock_control=bool(product_data.get('stockControl', 0)),
                    print_detail_pack=bool(product_data.get('printDetailPack', 0)),
                    state=bool(product_data.get('state', 0)),
                    prestashop_product_id=product_data.get('prestashopProductId', 0),
                    prestashop_attribute_id=product_data.get('presashopAttributeId', 0),
                    product_type_id=bsale_product_type.id,
                    product_type=bsale_product_type
                )
                tenant_session.add(bsale_product)
                tenant_session.commit()
                return bsale_product
            else:
                print(f"Advertencia: No se pudo obtener o crear el tipo de producto para el producto con ID {product_id}.")
                return None
        else:
            print(f"Advertencia: No se pudieron obtener los datos del producto con ID {product_id} desde la API.")
            return None

def get_or_create_bsale_variant(tenant_session, api_key: str, variant_id: int):
    """
    Obtiene una variante de Bsale por su ID o la crea si no existe,
    incluyendo la creación/obtención del producto asociado.

    Args:
        tenant_session: Sesión de SQLAlchemy.
        api_key: Clave de la API de Bsale.
        variant_id: ID de la variante.

    Returns:
        El objeto Bsale_Variant existente o recién creado.
    """
    bsale_variant = tenant_session.query(Bsale_Variant).filter_by(id=variant_id).first()
    if bsale_variant:
        return bsale_variant
    else:
        variant_data = get_bsale(api_key, endpoint=f"/v1/variants/{variant_id}.json")[0]
        if variant_data:
            product_data = variant_data.get('product')
            bsale_product = get_or_create_bsale_product(tenant_session, api_key, int(product_data['id'])) if product_data else None

            if bsale_product:
                bsale_variant = Bsale_Variant(
                    id=int(variant_data.get('id')),
                    description=variant_data.get('description'),
                    unlimited_stock=bool(variant_data.get('unlimitedStock', 0)),
                    allow_negative_stock=bool(variant_data.get('allowNegativeStock', 0)),
                    state=bool(variant_data.get('state', 0)),
                    bar_code=variant_data.get('barCode'),
                    code=variant_data.get('code'),
                    imagestion_center_cost=variant_data.get('imagestionCenterCost', 0),
                    imagestion_account=variant_data.get('imagestionAccount', 0),
                    imagestion_concept_cod=variant_data.get('imagestionConceptCod', 0),
                    imagestion_proyect_cod=variant_data.get('imagestionProyectCod', 0),
                    imagestion_category_cod=variant_data.get('imagestionCategoryCod', 0),
                    imagestion_product_id=variant_data.get('imagestionProductId', 0),
                    serial_number=bool(variant_data.get('serialNumber', 0)),
                    is_lot=bool(variant_data.get('isLot', 0)),
                    prestashop_combination_id=variant_data.get('prestashopCombinationId', 0),
                    prestashop_value_id=variant_data.get('prestashopValueId', 0),
                    product_id=bsale_product.id,
                    product=bsale_product
                )
                tenant_session.add(bsale_variant)
                tenant_session.commit()
                return bsale_variant
            else:
                print(f"Advertencia: No se pudo obtener o crear el producto para la variante con ID {variant_id}.")
                return None
        else:
            print(f"Advertencia: No se pudieron obtener los datos de la variante con ID {variant_id} desde la API.")
            return None

def get_or_create_bsale_price_list_details(tenant_session, api_key: str,price_list_id:int,variant_id:int):
    # Obtener y crear los detalles de la lista de precios
    db_variant = get_or_create_bsale_variant(tenant_session, api_key, variant_id)
    if not db_variant:
        print(f"Error: No se pudo obtener o crear la variante con ID {variant_id}. "
              f"No se puede procesar el detalle de la lista de precios.")
        return None
    
    existing_detail_by_biz_key = tenant_session.query(Bsale_Price_List_Detail).filter_by(
        price_list_id=price_list_id,
        variant_id=db_variant.id
    ).first()

    if existing_detail_by_biz_key:
        print(f"Detalle de lista de precios para lista {price_list_id} y variante {db_variant.id} "
              f"ya existe en BD (ID: {existing_detail_by_biz_key.id}). Devolviendo existente.")
        return existing_detail_by_biz_key

    details_data = get_bsale(api_key, endpoint=f"/v1/price_lists/{price_list_id}/details.json?variantid={variant_id}")
    if details_data:
        for detail in details_data:
            variant_data = detail.get('variant')
            bsale_variant = get_or_create_bsale_variant(tenant_session, api_key, int(variant_data['id']))
            existing_detail = tenant_session.query(Bsale_Price_List_Detail).filter_by(
                    variant_id=bsale_variant.id,
                    price_list_id=price_list_id
            ).first()
            if not existing_detail:
                bsale_price_list_detail = Bsale_Price_List_Detail(
                    id=detail.get('id'),
                    variant_value=detail.get('variantValue'),
                    variant_value_with_taxes=detail.get('variantValueWithTaxes'),
                    variant_id=bsale_variant.id,
                    price_list_id=price_list_id
                )
                tenant_session.add(bsale_price_list_detail)
                tenant_session.commit()

def get_or_create_bsale_price_list(tenant_session, api_key: str, price_list_id: int):
    """
    Obtiene una lista de precios de Bsale por su ID o la crea si no existe,
    incluyendo la creación/obtención de la moneda asociada y sus detalles.

    Args:
        tenant_session: Sesión de SQLAlchemy.
        api_key: Clave de la API de Bsale.
        price_list_id: ID de la lista de precios.

    Returns:
        El objeto Bsale_Price_List existente o recién creado.
    """

    bsale_price_list = tenant_session.query(Bsale_Price_List).filter_by(id=price_list_id).first()
    if bsale_price_list:
        return bsale_price_list
    else:
        price_list_data = get_bsale(api_key, endpoint=f"/v1/price_lists/{price_list_id}.json")[0]
        if price_list_data:
            coin_data = price_list_data.get('coin')
            bsale_coin = get_or_create_bsale_coin(tenant_session, coin_data, api_key) if coin_data else None

            if bsale_coin:
                bsale_price_list = Bsale_Price_List(
                    id=int(price_list_data.get('id')),
                    name=price_list_data.get('name'),
                    state=bool(price_list_data.get('state', 0)),
                    coin_id=bsale_coin.id,
                    coin=bsale_coin
                )
                tenant_session.add(bsale_price_list)
                tenant_session.commit()
                return bsale_price_list
            else:
                print(f"Advertencia: No se pudo obtener o crear la moneda para la lista de precios con ID {price_list_id}.")
                return None
        else:
            print(f"Advertencia: No se pudieron obtener los datos de la lista de precios con ID {price_list_id} desde la API.")
            return None

def get_or_create_bsale_office(office,tenant_session,api_key):
    off_id = office.get("id")
    bsale_office = tenant_session.query(Bsale_Office).filter_by(id=off_id).first()
    if bsale_office:
        return bsale_office
    else:
        office_data = get_bsale(api_key, endpoint=f"/v1/offices/{off_id}.json")[0]
        price_list = get_or_create_bsale_price_list(tenant_session,api_key,office_data.get('defaultPriceList'))

        new_office = Bsale_Office(
            id=office_data.get('id'),
            name=office_data.get('name'),
            description=office_data.get('description'),
            address=office_data.get('address'),
            latitude=office_data.get('latitude'),
            longitude=office_data.get('longitude'),
            is_virtual=bool(office_data.get('isVirtual', 0)),
            country=office_data.get('country'),
            municipality=office_data.get('municipality'),
            city=office_data.get('city'),
            zip_code=office_data.get('zipCode'),
            email=office_data.get('email'),
            cost_center=office_data.get('costCenter'),
            state=bool(office_data.get('state', 0)),
            imagestion_cellar_id=office_data.get('imagestionCellarId', 0),
            store=bool(office_data.get('store', 0)),
            default_price_list_id=price_list.id
        )
        tenant_session.add(new_office)
        tenant_session.commit()
        return new_office

def get_or_create_bsale_user(user_data, tenant_session, api_key: str):
    """
    Obtiene un usuario de Bsale por su ID o lo crea si no existe,
    incluyendo la creación/obtención de la oficina asociada.

    Args:
        user_data: Diccionario con la información del usuario de Bsale.
                   Debe contener al menos 'id', 'firstName', 'lastName', 'email',
                   'state', y un diccionario 'office' con la clave 'id'.
        tenant_session: Sesión de SQLAlchemy.
        api_key: Clave de la API de Bsale.

    Returns:
        El objeto Bsale_User existente o recién creado.
    """
    user_id = user_data.get('id')
    bsale_user = tenant_session.query(Bsale_User).filter_by(id=user_id).first()

    if bsale_user:
        return bsale_user
    else:
        office_data = user_data.get('office')
        if office_data and office_data.get('id'):
            bsale_office = get_or_create_bsale_office(office_data,tenant_session,api_key)
            if bsale_office:
                bsale_user = Bsale_User(
                    id=user_id,
                    first_name=user_data.get('firstName'),
                    last_name=user_data.get('lastName'),
                    email=user_data.get('email'),
                    state=bool(user_data.get('state', 0)),
                    office_id=bsale_office.id,
                    office=bsale_office
                )
                tenant_session.add(bsale_user)
                tenant_session.commit()
                return bsale_user
            else:
                print(f"Advertencia: No se pudo obtener o crear la oficina con ID {office_data['id']} para el usuario con ID {user_id}.")
                return None
        else:
            print(f"Advertencia: No se proporcionó la información de la oficina para el usuario con ID {user_id}.")
            return None

def get_or_create_bsale_seller(tenant_session, api_key: str, seller_input_data: dict):
    seller_id = seller_input_data.get('id')
    if not seller_id:
        print(f"Advertencia: No se proporcionó ID para el vendedor: {seller_input_data}")
        return None

    bsale_seller = tenant_session.query(Bsale_Seller).filter_by(id=seller_id).first()
    if bsale_seller:
        return bsale_seller
    else:
        first_name = seller_input_data.get('firstName', seller_input_data.get('first_name'))
        last_name = seller_input_data.get('lastName', seller_input_data.get('last_name'))
        
        print(f"Creando vendedor: ID={seller_id}, Nombre={first_name} {last_name}")
        new_seller = Bsale_Seller(
            id=seller_id,
            first_name=first_name,
            last_name=last_name
        )
        tenant_session.add(new_seller)
        return new_seller

def get_or_create_bsale_document(
        tenant_session,
        document_type,
        document_coin,
        document_price_list,
        document_sellers,
        document_user,
        document_office,
        document_api_data
        ):
    """
    Obtiene un documento de Bsale por su ID (de la API) o lo crea si no existe.
    Incluye la creación/obtención de sus entidades relacionadas.
    """
    doc_api_id = document_api_data.get('id')
    if not doc_api_id:
        print("Error: document_api_data no contiene un 'id' de documento.")
        return None

    bsale_document = tenant_session.query(Bsale_Document).filter_by(id=doc_api_id).first()
    if bsale_document:
        print(f"Documento con ID API {doc_api_id} ya existe en la BD local.")
        return bsale_document

    print(f"Creando documento con ID API {doc_api_id}...")

    # --- 2. Campos Directos del Documento ---
    generation_date_ts = document_api_data.get('generationDate')
    db_generation_date = datetime.fromtimestamp(generation_date_ts, tz=timezone.utc) if generation_date_ts else None
    if not db_generation_date:
        print("Error crítico: generationDate es requerida.")
        return None

    try:
        total_amount = Decimal(str(document_api_data.get('totalAmount', '0')))
        net_amount = Decimal(str(document_api_data.get('netAmount', '0')))
        tax_amount = Decimal(str(document_api_data.get('taxAmount', '0')))
        exempt_amount = Decimal(str(document_api_data.get('exemptAmount', '0')))
        not_exempt_amount = Decimal(str(document_api_data.get('notExemptAmount', '0')))
    except Exception as e:
        print(f"Error al convertir montos a Decimal: {e}. No se puede crear el documento.")
        return None

    # --- 3. Crear Instancia Bsale_Document ---
    new_document_header = Bsale_Document(
        id=doc_api_id,
        number=document_api_data.get('number'),
        date=db_generation_date,
        total_amount=total_amount,
        net_amount=net_amount,
        tax_amount=tax_amount,
        exempt_amount=exempt_amount,
        not_exempt_amount=not_exempt_amount,
        document_type_id=document_type.id,
        user_id=document_user.id,
        coin_id=document_coin.id,
        price_list_id=document_price_list.id,
        office_id=document_office.id,
    )

    # Asociar vendedores (relación muchos-a-muchos)
    associated_sellers_count = 0
    if document_sellers and isinstance(document_sellers, list):
        for seller_obj in document_sellers:
            if seller_obj and isinstance(seller_obj, Bsale_Seller):
                if seller_obj not in new_document_header.sellers: 
                    new_document_header.sellers.append(seller_obj)
                associated_sellers_count += 1 
        if associated_sellers_count > 0:
            print(f"Se asociaron {associated_sellers_count} vendedores al documento ID {doc_api_id}.")
        else:
            print(f"Advertencia: Aunque se pasó una lista de vendedores, no se asoció ninguno válido al documento ID {doc_api_id}.")
    else:
        print(f"Advertencia: No se proporcionó una lista de vendedores válida para el documento ID {doc_api_id}.")

    tenant_session.add(new_document_header)
    # --- 6. Commit Final ---
    try:
        tenant_session.commit()
        print(f"Documento ID API {doc_api_id} y sus relaciones creados/actualizados exitosamente.")
        return new_document_header
    except Exception as e:
        tenant_session.rollback()
        print(f"Error Crítico al hacer commit para documento ID API {doc_api_id}: {e}")
        return None

def get_or_create_bsale_tax(tenant_session, api_key: str, tax_id: int):
    tax_obj = tenant_session.query(Bsale_Tax).filter_by(id=tax_id).first()
    if tax_obj:
        return tax_obj
    else:
        tax_api_data = get_bsale(api_key, endpoint=f"/v1/taxes/{tax_id}.json")[0]
        if tax_api_data:
            tax_obj = Bsale_Tax(
            id=tax_api_data["id"],
            name=tax_api_data["name"],
            percentage=Decimal(tax_api_data["percentage"]),
            for_all_products=bool(tax_api_data["forAllProducts"]),
            ledger_account=tax_api_data["ledgerAccount"],
            code=tax_api_data["code"],
            state=bool(tax_api_data["state"]),
            over_tax=bool(tax_api_data["overTax"]),
            amount_tax=bool(tax_api_data["amountTax"]),
        )

        tenant_session.add(tax_obj)
        tenant_session.commit()

        return tax_obj

def get_or_create_document_tax_line(
    tenant_session,
    api_key: str, # Needed to get the Bsale_Tax definition if not already in your DB
    parent_document_id: int,
    doc_tax_api_data: dict
):
    doc_tax_api_id = doc_tax_api_data.get('id')
    if not doc_tax_api_id:
        print(f"Error: Document tax data is missing 'id': {doc_tax_api_data}")
        return None

    existing_doc_tax_line = tenant_session.query(Bsale_Document_Tax).filter_by(id=doc_tax_api_id).first()
    if existing_doc_tax_line:
        print(f"Document tax line ID {doc_tax_api_id} already exists.")
        return existing_doc_tax_line

    print(f"Creating document tax line ID {doc_tax_api_id}...")

    tax_summary_api_data = doc_tax_api_data.get('tax')
    if not tax_summary_api_data or not tax_summary_api_data.get('id'):
        print(f"Error: 'tax' information missing or incomplete in document tax data for ID {doc_tax_api_id}.")
        return None
    
    tax_definition_id = tax_summary_api_data.get('id')
    db_tax_definition = get_or_create_bsale_tax(tenant_session, api_key, tax_definition_id)

    if not db_tax_definition:
        print(f"Error: Could not get or create Bsale_Tax definition for tax_id {tax_definition_id}. Skipping document tax line {doc_tax_api_id}.")
        return None

    try:
        total_amount = Decimal(str(doc_tax_api_data.get('totalAmount', '0')))
        exempt_amount = Decimal(str(doc_tax_api_data.get('exemptAmount', '0')))
    except Exception as e:
        print(f"Error converting amounts for document tax line ID {doc_tax_api_id}: {e}")
        return None

    try:
        new_doc_tax_line = Bsale_Document_Tax(
            id=doc_tax_api_id,
            total_amount=total_amount,
            exempt_amount=exempt_amount,
            tax_id=db_tax_definition.id,
            document_id=parent_document_id
        )
        tenant_session.add(new_doc_tax_line)
        tenant_session.commit()
        print(f"Document tax line ID {doc_tax_api_id} for document ID {parent_document_id} added to session.")
        return new_doc_tax_line
    except Exception as e:
        print(f"Error instantiating Bsale_Document_Tax for ID {doc_tax_api_id}: {e}")
        return None

def get_or_create_document_detail_line(
    tenant_session,
    api_key: str,
    parent_document_id: int,
    detail_api_data: dict
):
    detail_api_id = detail_api_data.get('id')
    if not detail_api_id:
        print(f"Error: Document detail data is missing 'id': {detail_api_data}")
        return None

    existing_detail_line = tenant_session.query(Bsale_Document_Detail).filter_by(id=detail_api_id).first()
    if existing_detail_line:
        print(f"Document detail line ID {detail_api_id} already exists.")
        return existing_detail_line

    print(f"Creating document detail line ID {detail_api_id}...")

    variant_summary_api_data = detail_api_data.get('variant')
    if not variant_summary_api_data or not variant_summary_api_data.get('id'):
        print(f"Error: 'variant' information missing or incomplete in detail data for ID {detail_api_id}.")
        return None

    variant_id = variant_summary_api_data.get('id')
    db_variant = get_or_create_bsale_variant(tenant_session, api_key, variant_id)

    if not db_variant:
        print(f"Error: Could not get or create Bsale_Variant for variant_id {variant_id}. Skipping detail line {detail_api_id}.")
        return None

    try:
        quantity = Decimal(str(detail_api_data.get('quantity', '0')))
        net_unit_value = Decimal(str(detail_api_data.get('netUnitValue', '0')))
        net_unit_value_raw = Decimal(str(detail_api_data.get('netUnitValueRaw', '0')))
        total_unit_value = Decimal(str(detail_api_data.get('totalUnitValue', '0')))
        net_amount = Decimal(str(detail_api_data.get('netAmount', '0')))
        tax_amount_detail = Decimal(str(detail_api_data.get('taxAmount', '0')))
        total_amount_detail = Decimal(str(detail_api_data.get('totalAmount', '0')))
        net_discount = Decimal(str(detail_api_data.get('netDiscount', '0')))
        total_discount = Decimal(str(detail_api_data.get('totalDiscount', '0')))
    except Exception as e:
        print(f"Error converting amounts for document detail line ID {detail_api_id}: {e}")
        return None
    
    try:
        new_detail_line = Bsale_Document_Detail(
            id=detail_api_id,
            line_number=detail_api_data.get('lineNumber'),
            quantity=quantity,
            net_unit_value=net_unit_value,
            net_unit_value_raw=net_unit_value_raw,
            total_unit_value=total_unit_value,
            net_amount=net_amount,
            tax_amount=tax_amount_detail,
            total_amount=total_amount_detail,
            net_discount=net_discount,
            total_discount=total_discount,
            note=detail_api_data.get('note'),
            related_detail_id=detail_api_data.get('relatedDetailId'),
            variant_id=db_variant.id,
            document_id=parent_document_id
        )
        print
        tenant_session.add(new_detail_line)   
        tenant_session.commit()
        print(f"Document detail line ID {detail_api_id} for document ID {parent_document_id} added to session.")
        return new_detail_line
    except Exception as e:
        print(f"Error instantiating Bsale_Document_Detail for ID {detail_api_id}: {e}")
        return None

def sync_bsale_stock_record(
    tenant_session,
    api_key: str,
    variant_id_for_api_path: int,
    office_id_for_api_query: int
):
    endpoint = f"/v1/stocks.json?variantid={variant_id_for_api_path}&officeid={office_id_for_api_query}"
    stock_api_data = None
    try:
        api_response = get_bsale(api_key, endpoint=endpoint)[0]
        
        if isinstance(api_response, list):
            if api_response:
                stock_api_data = api_response[0]
            else:
                print(f"Respuesta de API vacía (lista) para {endpoint}")
                return None
        elif isinstance(api_response, dict):
             stock_api_data = api_response
        else:
            print(f"No se recibió una respuesta de API válida (ni lista ni dict) para {endpoint}. Respuesta: {api_response}")
            return None

        if not stock_api_data or not stock_api_data.get('id'):
            print(f"Respuesta de API no contiene datos de stock válidos o falta el 'id' principal. Respuesta: {stock_api_data}")
            return None

    except Exception as e:
        print(f"Error al solicitar stock a BsALE para endpoint {endpoint}: {e}")
        return None

    stock_record_pk_from_api = stock_api_data.get('id') 
    
    api_variant_info = stock_api_data.get('variant', {})
    actual_variant_id_str = api_variant_info.get('id')
    
    api_office_info = stock_api_data.get('office', {})
    actual_office_id_str = api_office_info.get('id')

    if not actual_variant_id_str or not actual_office_id_str:
        print(f"Respuesta de API para stock PK {stock_record_pk_from_api} no contiene 'variant.id' u 'office.id' válidos. Data: {stock_api_data}")
        return None
    
    try:
        actual_variant_id_int = int(actual_variant_id_str)
        actual_office_id_int = int(actual_office_id_str)
    except ValueError:
        print(f"Error: No se pudieron convertir variant.id ('{actual_variant_id_str}') o office.id ('{actual_office_id_str}') a enteros.")
        return None

    db_variant = get_or_create_bsale_variant(tenant_session, api_key, actual_variant_id_int)
    if not db_variant:
        print(f"Error: No se pudo obtener o crear Bsale_Variant con ID {actual_variant_id_int}. No se puede sincronizar stock.")
        return None

    db_office = get_or_create_bsale_office({'id': actual_office_id_int}, tenant_session, api_key)
    if not db_office:
        print(f"Error: No se pudo obtener o crear Bsale_Office con ID {actual_office_id_int}. No se puede sincronizar stock.")
        return None

    local_stock_record = tenant_session.query(Bsale_Stock).filter_by(id=stock_record_pk_from_api).first()

    try:
        quantity = Decimal(str(stock_api_data.get('quantity', '0')))
        quantity_reserved = Decimal(str(stock_api_data.get('quantityReserved', '0')))
        quantity_available = Decimal(str(stock_api_data.get('quantityAvailable', '0')))
    except Exception as e:
        print(f"Error al convertir cantidades de stock a Decimal para stock PK {stock_record_pk_from_api}: {e}")
        return None

    if local_stock_record:
        print(f"Actualizando stock local para ID de Stock {stock_record_pk_from_api} (Variante: {db_variant.id}, Oficina: {db_office.id})...")
        local_stock_record.quantity = quantity
        local_stock_record.quantity_reserved = quantity_reserved
        local_stock_record.quantity_available = quantity_available
        local_stock_record.variant_id = db_variant.id 
        local_stock_record.office_id = db_office.id
    else:
        print(f"Creando nuevo registro de stock local con ID de Stock {stock_record_pk_from_api} (Variante: {db_variant.id}, Oficina: {db_office.id})...")
        local_stock_record = Bsale_Stock(
            id=stock_record_pk_from_api,
            quantity=quantity,
            quantity_reserved=quantity_reserved,
            quantity_available=quantity_available,
            variant_id=db_variant.id,
            office_id=db_office.id
        )
        tenant_session.add(local_stock_record)
    
    try:
        tenant_session.commit()
        print(f"Stock para ID de Stock {stock_record_pk_from_api} (Variante: {db_variant.id}, Oficina: {db_office.id}) sincronizado y commiteado.")
        return local_stock_record
    except Exception as e:
        tenant_session.rollback()
        print(f"Error al hacer commit para stock ID {stock_record_pk_from_api}: {e}")
        return None

def sync_bsale_price_list_detail(
    tenant_session, 
    api_key: str,
    price_list_id_param: int,
    variant_id_param: int
) -> Bsale_Price_List_Detail | None:
    print(f"Sincronizando detalle de precio para lista {price_list_id_param}, variante {variant_id_param}...")
    api_endpoint = f"/v1/price_lists/{price_list_id_param}/details.json?variantid={variant_id_param}"
    detail_api_data = None
    try:
        raw_api_response = get_bsale(api_key, endpoint=api_endpoint)[0]
        if isinstance(raw_api_response, list) and raw_api_response:
            detail_api_data = raw_api_response[0]
        elif isinstance(raw_api_response, dict) and raw_api_response.get('id'):
            detail_api_data = raw_api_response
        
        if not detail_api_data:
            print(f"API no devolvió datos para el detalle de precio: lista {price_list_id_param}, variante {variant_id_param}.")
            return None
            
    except Exception as e:
        print(f"Error al llamar a la API ({api_endpoint}): {e}")
        return None

    api_detail_id = detail_api_data.get('id')
    api_variant_info = detail_api_data.get('variant', {})
    api_variant_id_str = api_variant_info.get('id')

    if not api_detail_id or not api_variant_id_str:
        print(f"Error: Respuesta de API para detalle de precio incompleta (falta id o variant.id). Datos: {detail_api_data}")
        return None

    try:
        api_variant_id_int = int(api_variant_id_str)
        api_variant_value = Decimal(str(detail_api_data.get('variantValue', '0')))
        api_variant_value_with_taxes = Decimal(str(detail_api_data.get('variantValueWithTaxes', '0')))
    except (ValueError, TypeError) as e:
        print(f"Error al convertir datos numéricos de la API para detalle {api_detail_id}: {e}")
        return None

    db_variant_obj_from_api = get_or_create_bsale_variant(tenant_session, api_key, api_variant_id_int)
    if not db_variant_obj_from_api:
        print(f"Error: No se pudo obtener/crear la variante con ID {api_variant_id_int} (desde API). "
              f"No se puede sincronizar el detalle de precio.")
        return None
    
    if api_variant_id_int != variant_id_param:
        print(f"Advertencia: El ID de variante solicitado ({variant_id_param}) difiere del ID de variante "
              f"en la respuesta del detalle de precio de la API ({api_variant_id_int}). "
              f"Se usará el ID de la respuesta API ({api_variant_id_int}) para la FK.")
    
    local_price_detail = tenant_session.query(Bsale_Price_List_Detail).filter_by(id=api_detail_id).first()

    if local_price_detail:
        print(f"Actualizando detalle de precio existente con ID de API {api_detail_id}...")
        updated_fields = False
        if local_price_detail.variant_value != api_variant_value:
            local_price_detail.variant_value = api_variant_value
            updated_fields = True
        if local_price_detail.variant_value_with_taxes != api_variant_value_with_taxes:
            local_price_detail.variant_value_with_taxes = api_variant_value_with_taxes
            updated_fields = True
        if local_price_detail.variant_id != db_variant_obj_from_api.id:
            local_price_detail.variant_id = db_variant_obj_from_api.id
            updated_fields = True
        if local_price_detail.price_list_id != price_list_id_param:
            local_price_detail.price_list_id = price_list_id_param
            updated_fields = True
        
        if updated_fields:
            tenant_session.add(local_price_detail)
            print(f"Detalle de precio ID {api_detail_id} actualizado.")
        else:
            print(f"Detalle de precio ID {api_detail_id} no necesitó actualización de valores.")
        return local_price_detail
    else:
        existing_by_biz_key = tenant_session.query(Bsale_Price_List_Detail).filter_by(
            variant_id=db_variant_obj_from_api.id, 
            price_list_id=price_list_id_param
        ).first()

        if existing_by_biz_key:
            print(f"Advertencia: Ya existe un detalle local (ID: {existing_by_biz_key.id}) para la variante "
                  f"{db_variant_obj_from_api.id} y lista de precios {price_list_id_param}, "
                  f"pero el ID de API es diferente ({api_detail_id}). "
                  f"Se actualizarán los valores del registro local existente.")
            # Actualizamos el registro encontrado por clave de negocio
            existing_by_biz_key.variant_value = api_variant_value
            existing_by_biz_key.variant_value_with_taxes = api_variant_value_with_taxes
            tenant_session.add(existing_by_biz_key)
            return existing_by_biz_key

        print(f"Creando nuevo detalle de lista de precios con ID de API {api_detail_id}...")
        try:
            new_price_detail = Bsale_Price_List_Detail(
                id=api_detail_id,
                variant_value=api_variant_value,
                variant_value_with_taxes=api_variant_value_with_taxes,
                variant_id=db_variant_obj_from_api.id,
                price_list_id=price_list_id_param
            )
            tenant_session.add(new_price_detail)
            tenant_session.commit()
            print(f"Nuevo detalle de precio ID {api_detail_id} añadido a la sesión.")
            return new_price_detail
        except Exception as e:
            print(f"Error al instanciar o añadir nuevo Bsale_Price_List_Detail con ID de API {api_detail_id}: {e}")
            return None

def process_document(document,api_key,company):
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)

    target_fields = [
        "document_type", "office", "user", "coin", "priceList",
        "references", "document_taxes", "details", "sellers"
    ]
    results_fields = {}

    for field in target_fields:
        if field in document and isinstance(document[field], dict) and "href" in document[field]:
            url = document[field]["href"].replace("http://legacy.api.bsale.com", "https://api.bsale.io")
            endpoint_rel = url.replace("https://api.bsale.io/v1/", "")
            print(endpoint_rel)
            try:
                datos_relacionados = get_bsale(api_key, endpoint=f"/v1/{endpoint_rel}")
                results_fields[field] = datos_relacionados
            except Exception as e:
                print(f"Error al obtener {field}: {e}")
    
    # Document Type
    doc_type = results_fields.get('document_type', [{}])[0]
    document_type = get_or_create_document_type(doc_type=doc_type, tenant_session=tenant_session)
    print("Tipo de documento listo")

    # User
    user = results_fields.get('user', [{}])[0]
    document_user = get_or_create_bsale_user(user_data=user,tenant_session=tenant_session,api_key=api_key)
    print("User listo")

    # Coin
    coin = results_fields.get('coin', [{}])[0]
    document_coin = get_or_create_bsale_coin(tenant_session=tenant_session,api_key=api_key, coin_data=coin)
    print("Coin listo")

    # Price List
    price_list = results_fields.get('priceList', [{}])[0]
    document_price_list = get_or_create_bsale_price_list(tenant_session=tenant_session,api_key=api_key,price_list_id=price_list.get('id'))
    print("Lista de precios Ok")

    # Sellers
    document_sellers = []
    for seller in results_fields.get('sellers', []):
        document_seller = get_or_create_bsale_seller(tenant_session=tenant_session,api_key=api_key,seller_input_data=seller)
        print("Vendedor ingresado")
        document_sellers.append(document_seller)

    # Oficina
    document_office = get_or_create_bsale_office(office=document['office'],tenant_session=tenant_session,api_key=api_key)
    
    # Crear documento
    new_document = get_or_create_bsale_document(
        tenant_session=tenant_session,
        document_type=document_type,
        document_coin=document_coin,
        document_price_list=document_price_list,
        document_sellers=document_sellers,
        document_user=document_user,
        document_office=document_office,
        document_api_data=document
    )
    print("Documento creado")
    # Document Taxes
    for document_tax in results_fields.get('document_taxes', []):
        get_or_create_document_tax_line(tenant_session,api_key,new_document.id,document_tax)
    print("Tax creado")

    # Details
    for detail in results_fields.get('details', []):
        variant_id = detail['variant'].get('id')
        get_or_create_document_detail_line(tenant_session,api_key,new_document.id,detail)
        get_or_create_bsale_price_list_details(tenant_session,api_key,price_list.get('id'),variant_id)
        sync_bsale_stock_record(tenant_session,api_key,variant_id,document_office.id)
    print("Detalle creado")
    return True

def update_stock(url,api_key,company):
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    variant_id = query_params.get('variant', [None])[0]
    office_id = query_params.get('office', [None])[0]
    office_obj = {"id": int(office_id)}

    office = get_or_create_bsale_office(office_obj,tenant_session,api_key)
    variant = get_or_create_bsale_variant(tenant_session,api_key,variant_id)

    sync_bsale_stock_record(tenant_session,api_key,variant.id,office.id)

def update_price(price_list_id,variant_id,api_key,company):
    tenant_db_url = f"postgresql://{company.db_user}:{company.db_password}@{company.db_host}/{company.db_name}"
    tenant_session = get_tenant_session(tenant_db_url)
    sync_bsale_price_list_detail(tenant_session,api_key,price_list_id,variant_id)

def sync_bsale_product(
    tenant_session,
    api_key: str,
    product_api_id: int
):
    """
    Sincroniza (actualiza o crea) un Bsale_Product.
    NO realiza commit internamente.
    """
    print(f"Sincronizando producto BSALE ID: {product_api_id}...")
    
    # 1. Obtener datos frescos del producto desde la API de BsALE
    product_api_data = None
    try:
        api_response_list = get_bsale(api_key, endpoint=f"/v1/products/{product_api_id}.json")[0]
        if api_response_list and isinstance(api_response_list, list):
            product_api_data = api_response_list[0]
        elif isinstance(api_response_list, dict):
            product_api_data = api_response_list
        
        if not product_api_data or product_api_data.get('id') != product_api_id:
            print(f"Error: No se encontraron datos válidos en la API para el producto ID {product_api_id}. Respuesta: {product_api_data}")
            return None
    except Exception as e:
        print(f"Error al obtener producto ID {product_api_id} de la API: {e}")
        return None

    product_type_api_info = product_api_data.get('product_type')
    db_product_type = None
    if product_type_api_info and product_type_api_info.get('id'):
        db_product_type = get_or_create_bsale_product_type(tenant_session, product_type_api_info, api_key)
    
    if not db_product_type:
        print(f"Error: No se pudo obtener/crear Bsale_Product_Type para producto ID {product_api_id}. "
              f"Datos de tipo de producto API: {product_type_api_info}")
        return None

    local_product = tenant_session.query(Bsale_Product).filter_by(id=product_api_id).first()

    try:
        name = product_api_data.get('name')
        description = product_api_data.get('description')
        classification = product_api_data.get('classification', 0)
        ledger_account = product_api_data.get('ledgerAccount')
        cost_center = product_api_data.get('costCenter')
        allow_decimal = bool(product_api_data.get('allowDecimal', False))
        stock_control = bool(product_api_data.get('stockControl', False))
        print_detail_pack = bool(product_api_data.get('printDetailPack', False))
        state = bool(product_api_data.get('state', True))
        prestashop_product_id = product_api_data.get('prestashopProductId', 0)
        prestashop_attribute_id = product_api_data.get('prestashopAttributeId', 0) 

        if name is None:
            print(f"Error: 'name' es nulo para producto API ID {product_api_id}")
            return None
            
    except Exception as e:
        print(f"Error al procesar campos del producto API ID {product_api_id}: {e}")
        return None

    if local_product:
        print(f"Actualizando producto existente ID {product_api_id}...")
        local_product.name = name
        local_product.description = description
        local_product.classification = classification
        local_product.ledger_account = ledger_account
        local_product.cost_center = cost_center
        local_product.allow_decimal = allow_decimal
        local_product.stock_control = stock_control
        local_product.print_detail_pack = print_detail_pack
        local_product.state = state
        local_product.prestashop_product_id = prestashop_product_id
        local_product.prestashop_attribute_id = prestashop_attribute_id
        local_product.product_type_id = db_product_type.id
        
        tenant_session.add(local_product)
        tenant_session.commit()
    else:
        print(f"Creando nuevo producto ID {product_api_id}...")
        local_product = Bsale_Product(
            id=product_api_id,
            name=name,
            description=description,
            classification=classification,
            ledger_account=ledger_account,
            cost_center=cost_center,
            allow_decimal=allow_decimal,
            stock_control=stock_control,
            print_detail_pack=print_detail_pack,
            state=state,
            prestashop_product_id=prestashop_product_id,
            prestashop_attribute_id=prestashop_attribute_id,
            product_type_id=db_product_type.id
        )
        tenant_session.add(local_product)
        tenant_session.commit()
        
    return local_product

def sync_bsale_variant(
    tenant_session,
    api_key: str,
    variant_api_id: int
):
    """
    Sincroniza (actualiza o crea) una Bsale_Variant.
    También se asegura de que el producto padre (Bsale_Product) esté sincronizado.
    NO realiza commit internamente.
    """
    print(f"Sincronizando variante BsALE ID: {variant_api_id}...")

    variant_api_data = None
    try:
        api_response_list = get_bsale(api_key, endpoint=f"/v1/variants/{variant_api_id}.json")
        if api_response_list and isinstance(api_response_list, list):
            variant_api_data = api_response_list[0]
        elif isinstance(api_response_list, dict):
            variant_api_data = api_response_list
            
        if not variant_api_data or variant_api_data.get('id') != variant_api_id:
            print(f"Error: No se encontraron datos válidos en la API para la variante ID {variant_api_id}. Respuesta: {variant_api_data}")
            return None
    except Exception as e:
        print(f"Error al obtener variante ID {variant_api_id} de la API: {e}")
        return None

    product_api_info = variant_api_data.get('product')
    db_product = None
    if product_api_info and product_api_info.get('id'):
        try:
            parent_product_api_id = int(product_api_info.get('id'))
            db_product = sync_bsale_product(tenant_session, api_key, parent_product_api_id)
        except ValueError:
            print(f"Error: ID de producto padre '{product_api_info.get('id')}' no es un entero válido para variante {variant_api_id}.")
            return None
    
    if not db_product:
        print(f"Error: No se pudo sincronizar el producto padre para la variante ID {variant_api_id}. "
              f"Datos de producto API: {product_api_info}")
        return None

    local_variant = tenant_session.query(Bsale_Variant).filter_by(id=variant_api_id).first()
    try:
        description = variant_api_data.get('description')
        unlimited_stock = bool(variant_api_data.get('unlimitedStock', False))
        allow_negative_stock = bool(variant_api_data.get('allowNegativeStock', False))
        state = bool(variant_api_data.get('state', True))
        bar_code = variant_api_data.get('barCode')
        code = variant_api_data.get('code')
        imagestion_center_cost = variant_api_data.get('imagestionCenterCost', 0)
        imagestion_account = variant_api_data.get('imagestionAccount', 0)
        imagestion_concept_cod = variant_api_data.get('imagestionConceptCod', 0)
        imagestion_proyect_cod = variant_api_data.get('imagestionProyectCod', 0)
        imagestion_category_cod = variant_api_data.get('imagestionCategoryCod', 0)
        imagestion_product_id = variant_api_data.get('imagestionProductId', 0)
        serial_number = bool(variant_api_data.get('serialNumber', False))
        is_lot = bool(variant_api_data.get('isLot', False))
        prestashop_combination_id = variant_api_data.get('prestashopCombinationId', 0)
        prestashop_value_id = variant_api_data.get('prestashopValueId', 0)
    except Exception as e:
        print(f"Error al procesar campos de la variante API ID {variant_api_id}: {e}")
        return None

    if local_variant:
        print(f"Actualizando variante existente ID {variant_api_id}...")
        local_variant.description = description
        local_variant.unlimited_stock = unlimited_stock
        local_variant.allow_negative_stock = allow_negative_stock
        local_variant.state = state
        local_variant.bar_code = bar_code
        local_variant.code = code
        local_variant.imagestion_center_cost = imagestion_center_cost
        local_variant.imagestion_account = imagestion_account
        local_variant.imagestion_concept_cod = imagestion_concept_cod
        local_variant.imagestion_proyect_cod = imagestion_proyect_cod
        local_variant.imagestion_category_cod = imagestion_category_cod
        local_variant.imagestion_product_id = imagestion_product_id
        local_variant.serial_number = serial_number
        local_variant.is_lot = is_lot
        local_variant.prestashop_combination_id = prestashop_combination_id
        local_variant.prestashop_value_id = prestashop_value_id
        local_variant.product_id = db_product.id
        
        tenant_session.add(local_variant)
        tenant_session.commit()
    else:
        print(f"Creando nueva variante ID {variant_api_id}...")
        local_variant = Bsale_Variant(
            id=variant_api_id,
            description=description,
            unlimited_stock=unlimited_stock,
            allow_negative_stock=allow_negative_stock,
            state=state,
            bar_code=bar_code,
            code=code,
            imagestion_center_cost=imagestion_center_cost,
            imagestion_account=imagestion_account,
            imagestion_concept_cod=imagestion_concept_cod,
            imagestion_proyect_cod=imagestion_proyect_cod,
            imagestion_category_cod=imagestion_category_cod,
            imagestion_product_id=imagestion_product_id,
            serial_number=serial_number,
            is_lot=is_lot,
            prestashop_combination_id=prestashop_combination_id,
            prestashop_value_id=prestashop_value_id,
            product_id=db_product.id
        )
        tenant_session.add(local_variant)
        tenant_session.commit()
        
    return local_variant