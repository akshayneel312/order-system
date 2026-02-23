from pydantic import BaseModel


class OrderCreate(BaseModel):
    item_name: str
    quantity: int
    price: float

class OrderResponse(OrderCreate):
    id: int
    status: str
    class Config:
        from_attributes = True

class UserCreate(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    id: int
    username: str
    class Config:
        orm_mode = True