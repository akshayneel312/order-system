import asyncio
from typing import List

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.auth import (create_access_token, get_password_hash, verify_password,
                      verify_token)
from app.database import AsyncSessionLocal, Base, engine, get_db
from app.manager import DatabaseManager
from app.models import OrderModel
from app.schemas import OrderCreate, OrderResponse, UserCreate, UserResponse
from app.service import consume_orders, kafka_service

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # retry logic for db
    max_retries = 5
    for i in range(max_retries):
        try:
            print(f"connect to database {i+1}/{max_retries}")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all) #create table
            print("DB connected, table created")
            break
        except Exception as e:
            print(f"DB connection failedv - {e}")
            if i == max_retries - 1:
                raise e
            await asyncio.sleep(5) # 5 sec wait
    
    #start producer
    await kafka_service.start()
    
    # # start Consumer
    # loop = asyncio.get_event_loop()
    # loop.call_later(10, lambda: loop.create_task(consume_orders(AsyncSessionLocal)))

    # REFACTOR: consume_orders no longer needs arguments!
    loop = asyncio.get_event_loop()
    loop.call_later(10, lambda: loop.create_task(consume_orders()))

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_service.stop()


@app.post("/orders/", response_model=OrderResponse)
async def create_order(order: OrderCreate, current_user: str = Depends(verify_token)):  #lock for verifying the user
    print(f"{current_user}' is creating an order.")
    #use db manager to handle DB logic
    async with DatabaseManager() as db:
        new_order = await db.save_order(order)
    #push to kafka
    await kafka_service.send_order_event(new_order.id, order.dict())

    return new_order


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: int):
    async with DatabaseManager() as db:
        order = await db.get_order_by_id(order_id)
        
    return order

# list orders
@app.get("/orders/", response_model=List[OrderResponse])
async def list_orders(limit: int = 10, offset: int = 0):
    async with DatabaseManager() as db:
        orders = await db.get_all_orders(limit, offset)
    return orders

#user register
@app.post("/register", response_model=UserResponse)
async def register_user(user: UserCreate):
    async with DatabaseManager() as db:
        #check if user exists already
        existing_user = await db.get_user_by_username(user.username)
        if existing_user:
            raise HTTPException(status_code=400, detail="Username already registered")
        
        #create hash of password
        hashed_pw = get_password_hash(user.password)
        
        #save to db
        new_user = await db.create_user(user, hashed_pw)
        return new_user
    
#user login
@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    async with DatabaseManager() as db:
        # get user
        user = await db.get_user_by_username(form_data.username)
        
        # check password
        if not user or not verify_password(form_data.password, user.hashed_password):
            raise HTTPException(status_code=401, detail="Incorrect username or password")
        
        #create access token
        access_token = create_access_token(data={"sub": user.username})
        return {"access_token": access_token, "token_type": "bearer"}