from sqlalchemy.future import select

from app.database import AsyncSessionLocal
from app.models import OrderModel, UserModel
from app.schemas import OrderCreate, UserCreate


class DatabaseManager:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = AsyncSessionLocal()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.session:
            await self.session.close()

    async def save_order(self, order_data: OrderCreate):
        new_order = OrderModel(
            item_name=order_data.item_name,
            quantity=order_data.quantity,
            price=order_data.price,
            status="PENDING"
        )
        self.session.add(new_order)
        await self.session.commit()
        await self.session.refresh(new_order)
        return new_order

    async def get_order_by_id(self, order_id: int):
        result = await self.session.execute(
            select(OrderModel).where(OrderModel.id == order_id)
        )
        return result.scalar_one_or_none()

    async def update_order_status(self, order_id: int, status: str):
        result = await self.session.execute(
            select(OrderModel).where(OrderModel.id == order_id)
        )
        order = result.scalar_one_or_none()
        if order:
            order.status = status
            await self.session.commit()
    
    async def get_all_orders(self, limit: int = 10, offset: int = 0):
        # Fetches a list of orders with pagination.
        result = await self.session.execute(
            select(OrderModel).offset(offset).limit(limit)
        )
        return result.scalars().all()

    #users
    async def get_user_by_username(self, username: str):
        result = await self.session.execute(
            select(UserModel).where(UserModel.username == username)
        )
        return result.scalar_one_or_none()

    async def create_user(self, user: UserCreate, hashed_password: str):
        new_user = UserModel(username=user.username, hashed_password=hashed_password)
        self.session.add(new_user)
        await self.session.commit()
        await self.session.refresh(new_user)
        return new_user