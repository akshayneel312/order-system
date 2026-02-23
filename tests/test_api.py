# tests/test_api.py
import asyncio
import uuid

import pytest
from httpx import AsyncClient

from app.main import app

#random user
TEST_USER = f"testuser_{uuid.uuid4().hex[:6]}"
TEST_PASS = "aks123"

#token
AUTH_TOKEN = ""

@pytest.mark.asyncio
class TestAuthentication:
    #Testing the Auth Flow (Register, Login, Security)

    async def test_1_register_user_success(self):
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/register", json={
                "username": TEST_USER,
                "password": TEST_PASS
            })
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == TEST_USER
        assert "id" in data

    async def test_2_register_duplicate_user_fails(self):
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/register", json={
                "username": TEST_USER,
                "password": TEST_PASS
            })
        assert response.status_code == 400
        assert response.json()["detail"] == "Username already registered"

    async def test_3_login_success_gets_token(self):
        global AUTH_TOKEN
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/login", data={
                "username": TEST_USER,
                "password": TEST_PASS
            })
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        AUTH_TOKEN = data["access_token"] #access_token

    async def test_4_login_wrong_password_fails(self):
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/login", data={
                "username": TEST_USER,
                "password": "wrongpassword"
            })
        assert response.status_code == 401

@pytest.mark.asyncio
class TestOrderProcessing:
    #Testing the Order Flow and Business Logic

    async def test_5_create_order_without_token_fails(self):
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/orders/", json={
                "item_name": "Ghost Item", "quantity": 1, "price": 10.0
            })
        assert response.status_code == 401 

    async def test_6_create_valid_order(self):
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        async with AsyncClient(base_url="http://localhost:8000") as client:
            response = await client.post("/orders/", headers=headers, json={
                "item_name": "Valid Laptop", 
                "quantity": 2, 
                "price": 1500.00
            })
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "PENDING"
        assert data["item_name"] == "Valid Laptop"

    async def test_7_business_logic_rejection(self):
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        async with AsyncClient(base_url="http://localhost:8000") as client:
            #Create a "bad" order
            response = await client.post("/orders/", headers=headers, json={
                "item_name": "Too Many Monitors", 
                "quantity": 11, # > 10 
                "price": 200.0
            })
            order_id = response.json()["id"]

            for _ in range(10):
                await asyncio.sleep(1)
                get_response = await client.get(f"/orders/{order_id}", headers=headers)
                if get_response.json()["status"] != "PENDING":
                    break #
            
            assert get_response.status_code == 200
            assert get_response.json()["status"] == "REJECTED"

    async def test_8_list_orders_pagination(self):
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        async with AsyncClient(base_url="http://localhost:8000") as client:
            # Fetch with limit=1
            response = await client.get("/orders/?limit=1&offset=0", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        assert type(data) == list
        assert len(data) <= 1 # Should only return 1 item due to limit