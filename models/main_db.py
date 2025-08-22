import enum
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table, Enum, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from database.main_db import Base
from sqlalchemy.sql import func

from models.tenant_db import MetricName

class CompanyRole(enum.Enum):
    user = "Usuario"
    company_admin = "Administrador de empresa"

user_company_association = Table('user_company_association', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete="CASCADE"), primary_key=True),
    Column('company_id', Integer, ForeignKey('companies.id', ondelete="CASCADE"), primary_key=True)
)

notification_user_association = Table('notification_user_association', Base.metadata,
    Column('notification_id', Integer, ForeignKey('notifications.id', ondelete="CASCADE"), primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id', ondelete="CASCADE"), primary_key=True),
    Column('is_read', Boolean, default=False, nullable=False)
)

notification_company_association = Table('notification_company_association', Base.metadata,
    Column('notification_id', Integer, ForeignKey('notifications.id', ondelete="CASCADE"), primary_key=True),
    Column('company_id', Integer, ForeignKey('companies.id', ondelete="CASCADE"), primary_key=True)
)

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String, index=True, nullable=True)
    phone_number = Column(String, nullable=True)
    country = Column(String, nullable=True)
    city = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    reset_password_token = Column(String, unique=True, index=True, nullable=True)
    reset_password_token_expires_at = Column(Integer, nullable=True)
    role = Column(Enum(CompanyRole), nullable=False, default=CompanyRole.user)
    companies = relationship(
        "Company",
        secondary=user_company_association,
        back_populates="users"
    )
    notifications = relationship(
        "Notification",
        secondary=notification_user_association,
        back_populates="users"
    )

class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    rut = Column(String, unique=True, index=True)
    selectedAvatar = Column(String, nullable=True) 
    db_name = Column(String, nullable=True)
    db_host = Column(String, nullable=True)
    db_user = Column(String, nullable=True)
    db_password = Column(String, nullable=True)
    api_key = Column(String, nullable=True)
    integration_id = Column(Integer, ForeignKey('integrations.id'), nullable=True)
    external_id = Column(String, nullable=True, index=True) 
    users = relationship(
        "User",
        secondary=user_company_association,
        back_populates="companies"
    )
    notifications = relationship(
        "Notification",
        secondary=notification_company_association,
        back_populates="companies"
    )
    integration = relationship("Integration", back_populates="companies")

class Integration(Base):
    __tablename__ = 'integrations'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    logo_url = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    companies = relationship("Company", back_populates="integration")

class Notification(Base):
    __tablename__ = 'notifications'

    id = Column(Integer, primary_key=True, index=True)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    users = relationship(
        "User",
        secondary=notification_user_association,
        back_populates="notifications"
    )
    companies = relationship(
        "Company",
        secondary=notification_company_association,
        back_populates="notifications"
    )

class MetricDisplayConfiguration(Base):
    __tablename__ = 'metric_display_configurations'

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete="CASCADE"), nullable=False)
    metric_id = Column(Enum(MetricName), nullable=False)
    name = Column(String, nullable=False)
    title = Column(String)
    short_name = Column(String)
    short_name_2 = Column(String)
    short_name_3 = Column(String)
    short_name_4 = Column(String)
    abbreviation = Column(String, nullable=True)
    information_1 = Column(Text, nullable=True)
    information_2 = Column(Text, nullable=True)
    
    background_color_1 = Column(String(7), nullable=True)
    text_color_1 = Column(String(7), nullable=True)
    text_color_2 = Column(String(7), nullable=True)
    background_color_3 = Column(String(7), nullable=True)
    text_color_3 = Column(String(7), nullable=True)
    background_color_4 = Column(String(7), nullable=True)
    text_color_4 = Column(String(7), nullable=True)

    show_counter = Column(Boolean, default=True, nullable=False)
    show_grid = Column(Boolean, default=True, nullable=False)
    show_distribution = Column(Boolean, default=True, nullable=False)
    show_products = Column(Boolean, default=True, nullable=False)

    product_operation = Column(String, default="greater")
    product_number = Column(Integer, default=3)
    
    __table_args__ = (UniqueConstraint('company_id', 'metric_id', name='_company_metric_id_uc'),)