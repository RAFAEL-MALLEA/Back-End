from sqlalchemy import Boolean, Column, Integer, String, Numeric, DateTime, ForeignKey, Table
from sqlalchemy.orm import declarative_base, relationship

TenantBase = declarative_base()

class Bsale_Document_Type(TenantBase):
    __tablename__ = 'bsale_document_type'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String)

class Bsale_Coin(TenantBase):
    __tablename__ = 'bsale_coin'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    decimals = Column(Integer, nullable=False)
    round_decimals = Column(Integer, nullable=False)
    total_round = Column(Boolean, nullable=False)
    name_in_words = Column(String, nullable=False)
    iso_code = Column(String, nullable=True)

class Bsale_Price_List(TenantBase):
    __tablename__ = 'bsale_price_list'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    state = Column(Boolean, nullable=False)
    coin_id = Column(Integer, ForeignKey('bsale_coin.id'), nullable=False)
    coin = relationship("Bsale_Coin")

class Bsale_Tax(TenantBase):
    __tablename__ = 'bsale_tax'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    percentage = Column(Numeric(5, 2), nullable=False)
    for_all_products = Column(Boolean, nullable=False)
    ledger_account = Column(String)
    code = Column(String)
    state = Column(Boolean, nullable=False)
    over_tax = Column(Boolean, nullable=False)
    amount_tax = Column(Boolean, nullable=False)

class Bsale_Product_Type(TenantBase):
    __tablename__ = 'bsale_product_type'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    is_editable = Column(Boolean, nullable=False)
    state = Column(Boolean, nullable=False)
    imagestion_category_id = Column(Integer, nullable=False)
    prestashop_category_id = Column(Integer, nullable=False)


class Bsale_Product(TenantBase):
    __tablename__ = 'bsale_product'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    description = Column(String)
    classification = Column(Integer, nullable=False)
    ledger_account = Column(String)
    cost_center = Column(String)
    allow_decimal = Column(Boolean, nullable=False)
    stock_control = Column(Boolean, nullable=False)
    print_detail_pack = Column(Boolean, nullable=False)
    state = Column(Boolean, nullable=False)
    prestashop_product_id = Column(Integer, nullable=False)
    prestashop_attribute_id = Column(Integer, nullable=False)
    product_type_id = Column(Integer, ForeignKey('bsale_product_type.id'), nullable=False)
    product_type = relationship("Bsale_Product_Type")


class Bsale_Variant(TenantBase):
    __tablename__ = 'bsale_variant'
    id = Column(Integer, primary_key=True, autoincrement=False)
    description = Column(String)
    unlimited_stock = Column(Boolean, nullable=False)
    allow_negative_stock = Column(Boolean, nullable=False)
    state = Column(Boolean, nullable=False)
    bar_code = Column(String)
    code = Column(String)
    imagestion_center_cost = Column(Integer, nullable=False)
    imagestion_account = Column(Integer, nullable=False)
    imagestion_concept_cod = Column(Integer, nullable=False)
    imagestion_proyect_cod = Column(Integer, nullable=False)
    imagestion_category_cod = Column(Integer, nullable=False)
    imagestion_product_id = Column(Integer, nullable=False)
    serial_number = Column(Boolean, nullable=False)
    is_lot = Column(Boolean, nullable=False)
    prestashop_combination_id = Column(Integer, nullable=False)
    prestashop_value_id = Column(Integer, nullable=False)
    product_id = Column(Integer, ForeignKey('bsale_product.id'), nullable=False)
    product = relationship("Bsale_Product")


class Bsale_Price_List_Detail(TenantBase):
    __tablename__ = 'bsale_price_list_detail'
    id = Column(Integer, primary_key=True, autoincrement=False)
    variant_value = Column(Numeric(12, 6), nullable=False)
    variant_value_with_taxes = Column(Numeric(12, 6), nullable=False)
    variant_id = Column(Integer, ForeignKey('bsale_variant.id'), nullable=False)
    price_list_id = Column(Integer, ForeignKey('bsale_price_list.id'), nullable=False)
    variant = relationship("Bsale_Variant")
    price_list = relationship("Bsale_Price_List")


class Bsale_Office(TenantBase):
    __tablename__ = 'bsale_office'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False)
    description = Column(String)
    address = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    is_virtual = Column(Boolean, nullable=False)
    country = Column(String)
    municipality = Column(String)
    city = Column(String)
    zip_code = Column(String)
    email = Column(String)
    cost_center = Column(String)
    state = Column(Boolean, nullable=False)
    imagestion_cellar_id = Column(Integer, nullable=False)
    store = Column(Boolean, nullable=False)
    default_price_list_id = Column(Integer, ForeignKey('bsale_price_list.id'), nullable=False)
    default_price_list = relationship("Bsale_Price_List")


class Bsale_User(TenantBase):
    __tablename__ = 'bsale_user'
    id = Column(Integer, primary_key=True, autoincrement=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    state = Column(Boolean, nullable=False)
    office_id = Column(Integer, ForeignKey('bsale_office.id'), nullable=False)
    office = relationship("Bsale_Office")


class Bsale_Stock(TenantBase):
    __tablename__ = 'bsale_stock'
    id = Column(Integer, primary_key=True, autoincrement=False)
    quantity = Column(Numeric(12, 3), nullable=False)
    quantity_reserved = Column(Numeric(12, 3), nullable=False)
    quantity_available = Column(Numeric(12, 3), nullable=False)
    variant_id = Column(Integer, ForeignKey('bsale_variant.id'), nullable=False)
    office_id = Column(Integer, ForeignKey('bsale_office.id'), nullable=False)
    variant = relationship("Bsale_Variant")
    office = relationship("Bsale_Office")

bsale_document_seller_association = Table(
    'bsale_document_seller_association', TenantBase.metadata,
    Column('document_id', Integer, ForeignKey('bsale_document.id'), primary_key=True),
    Column('seller_id', Integer, ForeignKey('bsale_seller.id'), primary_key=True)
)

class Bsale_Seller(TenantBase):
    __tablename__ = 'bsale_seller'
    id = Column(Integer, primary_key=True, autoincrement=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    
    documents = relationship(
        "Bsale_Document",
        secondary=bsale_document_seller_association,
        back_populates="sellers"
    )


class Bsale_Document(TenantBase):
    __tablename__ = 'bsale_document'
    id = Column(Integer, primary_key=True, autoincrement=False) 
    number = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    total_amount = Column(Numeric(12, 3), nullable=False)
    net_amount = Column(Numeric(12, 3), nullable=False)
    tax_amount = Column(Numeric(12, 3), nullable=False)
    exempt_amount = Column(Numeric(12, 3), nullable=False)
    not_exempt_amount = Column(Numeric(12, 3), nullable=False)
    document_type_id = Column(Integer, ForeignKey('bsale_document_type.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('bsale_user.id'), nullable=False)
    coin_id = Column(Integer, ForeignKey('bsale_coin.id'), nullable=False)
    price_list_id = Column(Integer, ForeignKey('bsale_price_list.id'), nullable=False)
    office_id = Column(Integer, ForeignKey('bsale_office.id'), nullable=False)
    document_type = relationship("Bsale_Document_Type")
    user = relationship("Bsale_User")
    coin = relationship("Bsale_Coin")
    price_list = relationship("Bsale_Price_List")
    office = relationship("Bsale_Office")
    sellers = relationship(
        "Bsale_Seller",
        secondary=bsale_document_seller_association,
        back_populates="documents"
    )
    details = relationship("Bsale_Document_Detail", back_populates="document_header")
    document_taxes_assoc = relationship("Bsale_Document_Tax", back_populates="document_header")


class Bsale_Document_Tax(TenantBase):
    __tablename__ = 'bsale_document_tax'
    id = Column(Integer, primary_key=True, autoincrement=False)
    total_amount = Column(Numeric(12, 2), nullable=False)
    exempt_amount = Column(Numeric(12, 2), nullable=False)
    tax_id = Column(Integer, ForeignKey('bsale_tax.id'), nullable=False)
    document_id = Column(Integer, ForeignKey('bsale_document.id'), nullable=False)
    tax = relationship("Bsale_Tax")
    document_header = relationship("Bsale_Document", back_populates="document_taxes_assoc")

class Bsale_Document_Detail(TenantBase):
    __tablename__ = 'bsale_document_detail'
    id = Column(Integer, primary_key=True, autoincrement=False)
    line_number = Column(Integer, nullable=False)
    quantity = Column(Numeric(10, 2), nullable=False)
    net_unit_value = Column(Numeric(12, 2), nullable=False)
    net_unit_value_raw = Column(Numeric(20, 8), nullable=False)
    total_unit_value = Column(Numeric(12, 2), nullable=False)
    net_amount = Column(Numeric(12, 2), nullable=False)
    tax_amount = Column(Numeric(12, 2), nullable=False)
    total_amount = Column(Numeric(12, 2), nullable=False)
    net_discount = Column(Numeric(12, 2), nullable=False)
    total_discount = Column(Numeric(12, 2), nullable=False)
    note = Column(String)
    related_detail_id = Column(Integer)
    variant_id = Column(Integer, ForeignKey('bsale_variant.id'), nullable=False)
    document_id = Column(Integer, ForeignKey('bsale_document.id'), nullable=False)
    variant = relationship("Bsale_Variant")
    document_header = relationship("Bsale_Document", back_populates="details")


class Bsale_Return(TenantBase):
    __tablename__ = 'bsale_return'
    id = Column(Integer, primary_key=True, autoincrement=False)
    code = Column(String)
    return_date = Column(DateTime, nullable=False)
    motive = Column(String)
    type = Column(Integer)
    price_adjustment = Column(Numeric(12, 2))
    edit_texts = Column(Integer)
    amount = Column(Numeric(12, 2), nullable=False)
    office_id = Column(Integer, ForeignKey('bsale_office.id'))
    user_id = Column(Integer, ForeignKey('bsale_user.id'))
    reference_document_id = Column(Integer, ForeignKey('bsale_document.id'))
    office = relationship("Bsale_Office")
    user = relationship("Bsale_User")
    reference_document = relationship("Bsale_Document")
    details = relationship("Bsale_Return_Detail", back_populates="return_header")

class Bsale_Return_Detail(TenantBase):
    __tablename__ = 'bsale_return_detail'
    id = Column(Integer, primary_key=True, autoincrement=False)
    quantity = Column(Numeric(10, 3), nullable=False)
    quantity_dev_stock = Column(Numeric(10, 3))
    variant_stock = Column(Numeric(10, 3))
    variant_cost = Column(Numeric(12, 2))
    return_id = Column(Integer, ForeignKey('bsale_return.id'), nullable=False)
    return_header = relationship("Bsale_Return", back_populates="details")

    def __repr__(self):
        return f"<Bsale_Return_Detail(id={self.id}, quantity={self.quantity})>"

class Bsale_Stock_Reception(TenantBase):
    __tablename__ = 'bsale_stock_reception'
    id = Column(Integer, primary_key=True, autoincrement=False)
    admission_date = Column(DateTime, nullable=False)
    document_name = Column(String)
    document_number = Column(String)
    note = Column(String)
    imagestion_cct_id = Column(Integer)
    imagestion_cc_description = Column(String)
    internal_dispatch_id = Column(Integer)
    update_stock = Column(Boolean)
    office_id = Column(Integer, ForeignKey('bsale_office.id'))
    user_id = Column(Integer, ForeignKey('bsale_user.id'))
    office = relationship("Bsale_Office")
    user = relationship("Bsale_User")
    details = relationship("Bsale_Stock_Reception_Detail", back_populates="reception_header")

    def __repr__(self):
        return f"<Bsale_Stock_Reception(id={self.id}, admission_date='{self.admission_date}')>"


class Bsale_Stock_Reception_Detail(TenantBase):
    __tablename__ = 'bsale_stock_reception_detail'
    id = Column(Integer, primary_key=True, autoincrement=False)
    quantity = Column(Numeric(10, 3), nullable=False)
    cost = Column(Numeric(12, 2))
    variant_stock = Column(Numeric(10, 3))
    serial_number = Column(String, nullable=True)
    reception_id = Column(Integer, ForeignKey('bsale_stock_reception.id'), nullable=False)
    variant_id = Column(Integer, ForeignKey('bsale_variant.id'), nullable=False)
    reception_header = relationship("Bsale_Stock_Reception", back_populates="details")
    variant = relationship("Bsale_Variant")

    def __repr__(self):
        return f"<Bsale_Stock_Reception_Detail(id={self.id}, reception_id={self.reception_id}, variant_id={self.variant_id}, quantity={self.quantity})>"