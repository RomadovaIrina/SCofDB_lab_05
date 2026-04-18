"""
LAB 05: Демонстрация неконсистентности кэша.
"""

import pytest

import json
import uuid

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.infrastructure.cache_keys import order_card_key



DATABASE_URL = "postgresql+asyncpg://postgres:postgres@db:5432/marketplace"
REDIS_URL = "redis://redis:6379/0"


@pytest.fixture
async def test_engine():
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session(test_engine):
    Session = sessionmaker(
        test_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    async with Session() as session:
        yield session


async def create_test_order(db_session: AsyncSession):
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    await db_session.execute(
        text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, NOW())
        """),
        {
            "id": user_id,
            "email": f"fresh_{str(user_id)[:8]}@dummy.com",
            "name": "Fresh User",
        }
    )

    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, 'created', :total_amount, NOW())
        """),
        {
            "id": order_id,
            "user_id": user_id,
            "total_amount": 100.0,
        }
    )

    await db_session.execute(
        text("""
            INSERT INTO order_items (id, order_id, product_name, price, quantity)
            VALUES (uuid_generate_v4(), :order_id, 'Test Product', 100.0, 1)
        """),
        {"order_id": order_id}
    )

    await db_session.execute(
        text("""
            INSERT INTO order_status_history (id, order_id, status, changed_at)
            VALUES (uuid_generate_v4(), :order_id, 'created', NOW())
        """),
        {"order_id": order_id}
    )

    await db_session.commit()

    return str(order_id), str(user_id)


async def delete_test_order(db_session: AsyncSession, order_id: str, user_id: str) -> None:
    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM order_items WHERE order_id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :user_id"),
        {"user_id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_stale_order_card_when_db_updated_without_invalidation(db_session):
    """
    TODO: Реализовать сценарий:
    1) Прогреть кэш карточки заказа (GET /api/cache-demo/orders/{id}/card?use_cache=true).
    2) Изменить заказ в БД через endpoint mutate-without-invalidation.
    3) Повторно запросить карточку с use_cache=true.
    4) Проверить, что клиент получает stale данные из кэша.
    """

    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    async with AsyncClient(app=app, base_url="http://test") as client:
        o_id, u_id = await create_test_order(db_session)

        kery = order_card_key(o_id)
        print(f"Generated cache key: {kery}")

        responce_1 = await client.get(
            f"/api/cache-demo/orders/{o_id}/card",
            params={"use_cache": "true"}
        )
        assert responce_1.status_code == 200

        data_before_cache = responce_1.json()
        assert data_before_cache["total_amount"] == 100.0

        key_before_cache = await redis.get(kery)
        print(f' cache key before mutation: {key_before_cache}')

        responce_2 = await client.post(
            f"/api/cache-demo/orders/{o_id}/mutate-without-invalidation",
            json={"new_total_amount": 250.0}
        )
        assert responce_2.status_code == 200

        data_mutation = responce_2.json()
        assert data_mutation["new_total_amount"] == 250.0
        print(f' mutation response: {data_mutation}')

        responce_3 = await client.get(
            f"/api/cache-demo/orders/{o_id}/card",
            params={"use_cache": "true"}
        )
        assert responce_3.status_code == 200

        data_after_cache = responce_3.json()
        assert data_after_cache["total_amount"] == 100.0
        print(f' data after cache: {data_after_cache}')

        key_after_cache = await redis.get(kery)
        print(f' cache key after mutation: {key_after_cache}')

        assert key_after_cache is not None

        cached_parsed = json.loads(key_after_cache)
        assert cached_parsed["total_amount"] == 100.0

        responce_4 = await client.get(
            f"/api/cache-demo/orders/{o_id}/card",
            params={"use_cache": "false"}
        )
        assert responce_4.status_code == 200

        data_after_no_cache = responce_4.json()
        assert data_after_no_cache["total_amount"] == 250.0
        print(f' data after no cache: {data_after_no_cache}')

        await delete_test_order(db_session, o_id, u_id)
    
    await redis.flushall()
    await redis.aclose()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
