from pydantic_settings import BaseSettings
from pydantic import EmailStr
from datetime import timedelta

class Settings(BaseSettings):
    PROJECT_NAME: str = "Inventaria"
    PROJECT_VERSION: str = "1.0.0"

    # JWT Settings
    SECRET_KEY: str = "XM01HG29R2HP"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 10080
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30
    PASSWORD_RESET_TOKEN_EXPIRE_MINUTES: int = 15

    # AWS SES Settings
    AWS_REGION_NAME: str = "us-east-1"
    SENDER_EMAIL: EmailStr = "alertas@inventaria.cl"
    
    FRONTEND_URL: str = "http://localhost:3000"

    class Config:
        case_sensitive = True

settings = Settings()