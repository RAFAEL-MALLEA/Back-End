from pydantic import BaseModel

class ProductCreate(BaseModel):
    name: str
    sku: str

class ProductOut(ProductCreate):
    id: int
    class Config:
        orm_mode = True