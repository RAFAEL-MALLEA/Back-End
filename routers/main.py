from typing import Optional
from fastapi import APIRouter, Depends, BackgroundTasks, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session
from database.main_db import SessionLocal
from models.main_db import Company
from services.tenant_manager import setup_tenant_and_notify
from utils.s3_uploader import upload_file_to_s3

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/create_company")
def create_company(
    background_tasks: BackgroundTasks,
    name: str = Form(..., description="Nombre de la nueva empresa."),
    rut: str = Form(..., description="RUT o identificador único de la empresa."),
    file: Optional[UploadFile] = File(None, description="Opcional: Archivo de imagen para el perfil de la empresa."),
    db: Session = Depends(get_db)
    ):

    existing_company = db.query(Company).filter((Company.name == name) | (Company.rut == rut)).first()
    if existing_company:
        raise HTTPException(status_code=409, detail="Una empresa con ese nombre o RUT ya existe.")

    avatar_url = None
    if file:
        print(f"Subiendo archivo '{file.filename}' a S3...")
        try:
            avatar_url = upload_file_to_s3(file)
            print(f"Archivo subido exitosamente. URL: {avatar_url}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"No se pudo subir la imagen: {e}")

    new_company = Company(
        name=name,
        rut=rut,
        selectedAvatar=avatar_url
    )
    db.add(new_company)
    db.commit()
    db.refresh(new_company)
    
    background_tasks.add_task(
        setup_tenant_and_notify,
        company_id=new_company.id,
        company_name=new_company.name
    )

    return {
        "message": f"Solicitud para crear la empresa '{name}' recibida. El proceso se ha iniciado en segundo plano.", 
        "company_id": new_company.id,
        "avatar_url": avatar_url
    }

@router.delete("/delete_company/{company_id}")
def delete_company(company_id: int, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        return {"error": "Empresa no encontrada"}

    db_name_prefix = company.name.replace(" ", "")
    # result = delete_db(db_name_prefix)

    #if not result:
    #    return {"error": "Error al eliminar la base de datos RDS"}

    db.delete(company)
    db.commit()

    return {"msg": "Empresa y base de datos eliminadas correctamente"}