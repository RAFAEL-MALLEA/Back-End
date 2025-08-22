import boto3
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta

from database.main_db import get_db
from schemas.user_schemas import UserBase, UserCreate, UserRegister, UserResponse
from schemas.token_schemas import RefreshTokenRequest, Token, PasswordResetRequest, PasswordReset, UserLoginRequest
from crud import company_crud, user_crud
from core.security import (
    create_access_token,
    create_refresh_token,
    verify_password,
    create_password_reset_token,
    verify_password_reset_token,
    verify_refresh_token
)
from core.config import settings
from auth.auth_bearer import get_current_active_user
from botocore.exceptions import ClientError

async def send_password_reset_email(email_to: str, token: str):
    """
    Envía un correo de restablecimiento de contraseña usando AWS SES.
    """
    ses_client = boto3.client(
            'ses',
            region_name=settings.AWS_REGION_NAME,
    )

    reset_link = f"{settings.FRONTEND_URL}/reset-password?token={token}"
    subject = "Restablecimiento de Contraseña Solicitado"
    
    body_html = f"""
    <html>
    <head></head>
    <body>
      <h1>Solicitud de Restablecimiento de Contraseña</h1>
      <p>Hola,</p>
      <p>Has solicitado restablecer tu contraseña para tu cuenta en {settings.PROJECT_NAME}.</p>
      <p>Por favor, haz clic en el siguiente enlace para continuar:</p>
      <p><a href="{reset_link}">Restablecer mi contraseña</a></p>
      <p>Si no solicitaste esto, por favor ignora este correo.</p>
      <p>El enlace es válido por {settings.PASSWORD_RESET_TOKEN_EXPIRE_MINUTES} minutos.</p>
      <br>
      <p>Saludos,</p>
      <p>El equipo de {settings.PROJECT_NAME}</p>
    </body>
    </html>
    """
    
    # Cuerpo del correo en Texto Plano (para clientes de correo que no soportan HTML)
    body_text = f"""
    Solicitud de Restablecimiento de Contraseña

    Hola,

    Has solicitado restablecer tu contraseña para tu cuenta en {settings.PROJECT_NAME}.
    Por favor, copia y pega el siguiente enlace en tu navegador para continuar:
    {reset_link}

    Si no solicitaste esto, por favor ignora este correo.
    El enlace es válido por {settings.PASSWORD_RESET_TOKEN_EXPIRE_MINUTES} minutos.

    Saludos,
    El equipo de {settings.PROJECT_NAME}
    """

    try:
        response = ses_client.send_email(
            Source=settings.SENDER_EMAIL,
            Destination={
                'ToAddresses': [
                    email_to,
                ]
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Html': {
                        'Data': body_html,
                        'Charset': 'UTF-8'
                    },
                    'Text': {
                        'Data': body_text,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        print(f"Email enviado a {email_to}! Message ID: {response.get('MessageId')}")
    except ClientError as e:
        print(f"Error al enviar correo a {email_to}: {e.response['Error']['Message']}")
    except Exception as e_gen:
        print(f"Error general al enviar correo: {str(e_gen)}")


router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserResponse)
async def register_user(user_in: UserRegister, db: Session = Depends(get_db)):
    db_user_exists = user_crud.get_user_by_email(db, email=user_in.email)
    if db_user_exists:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    company_ids_for_creation = []
    if user_in.company_id:
        company = company_crud.get_company_by_id(db, company_id=user_in.company_id)
        if not company:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Company with ID {user_in.company_id} not found. User registration failed."
            )
        company_ids_for_creation.append(company.id)
    user_create_payload = UserCreate(
        email=user_in.email,
        password=user_in.password,
        full_name=user_in.full_name,
        phone_number=user_in.phone_number,
        country=user_in.country,
        city=user_in.city,
        company_ids=company_ids_for_creation,
    )
    
    new_user = user_crud.create_user(db=db, user_in=user_create_payload)

    if not new_user:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not create user")
        
    return new_user

@router.post("/login", response_model=Token)
async def login_for_access_token(
    login_data: UserLoginRequest,
    db: Session = Depends(get_db)
):
    user = user_crud.get_user_by_email(db, email=login_data.email)
    if not user or not verify_password(login_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        subject=user.email, expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        subject=user.email
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token": refresh_token
    }

@router.post("/password-reset-request")
async def request_password_reset(
    request_body: PasswordResetRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    user = user_crud.get_user_by_email(db, email=request_body.email)
    if not user:
        print(f"Password reset requested for non-existent email: {request_body.email}")
    else:
        password_reset_token = create_password_reset_token(email=user.email)
        background_tasks.add_task(send_password_reset_email, user.email, password_reset_token)

    return {"msg": "If an account with that email exists, a password reset link has been sent."}


@router.post("/password-reset")
async def reset_password(reset_data: PasswordReset, db: Session = Depends(get_db)):
    email = verify_password_reset_token(reset_data.token)
    if not email:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired token")

    user = user_crud.get_user_by_email(db, email=email)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")

    user_crud.change_password(db=db, user=user, new_password=reset_data.new_password)
    return {"msg": "Password updated successfully"}

@router.post("/refresh", response_model=Token)
async def refresh_access_token(
    token_data: RefreshTokenRequest,
    db: Session = Depends(get_db)
):
    incoming_refresh_token = token_data.refresh_token
    
    email = verify_refresh_token(incoming_refresh_token)
    
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    user = user_crud.get_user_by_email(db, email=email)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found for refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user",
        )
        
    new_access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    new_access_token = create_access_token(
        subject=user.email, expires_delta=new_access_token_expires
    )
    
    new_refresh_token = create_refresh_token(subject=user.email)
    
    return {
        "access_token": new_access_token,
        "token_type": "bearer",
        "refresh_token": new_refresh_token,
    }

# Ruta de ejemplo protegida
@router.get("/users/me", response_model=UserBase)
async def read_users_me(current_user: UserBase = Depends(get_current_active_user)):
    """
    Test endpoint to get current user.
    """
    return current_user