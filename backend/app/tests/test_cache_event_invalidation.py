"""
LAB 05: Проверка починки через событийную инвалидацию.
"""

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
async def test_order_card_is_fresh_after_event_invalidation(db_session):
    """
    TODO: Реализовать сценарий:
    1) Прогреть кэш карточки заказа.
    2) Изменить заказ через mutate-with-event-invalidation.
    3) Убедиться, что ключ карточки инвалидирован.
    4) Повторный GET возвращает свежие данные из БД, а не stale cache.
    """

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    await redis.flushall()

    async with AsyncClient(app=app, base_url="http://test") as client:
        o_id, u_id = await create_test_order(db_session)


        key = order_card_key(o_id)
        print(f"\n KEY {key}")

        responce_1 = await client.get(
            f"/api/cache-demo/orders/{o_id}/card",
            params={"use_cache": True}
        )
        assert responce_1.status_code == 200
        data_1 = responce_1.json()
        assert data_1["status"] == "created"
        print(f'\n STATUS IS  {data_1["status"]}')

        keys = await redis.keys("*")
        print(f"\n REDIS KEYS: {keys}")

        cached_before_inv = await redis.get(key)
        print(f"\n cached_before_inv {cached_before_inv}")
        assert cached_before_inv is not None

        responce_2 = await client.post(
            f"/api/cache-demo/orders/{o_id}/mutate-with-event-invalidation",
            json={"new_total_amount": 250.0}
        )
        assert responce_2.status_code == 200


        cached_after_inv = await redis.get(key)
        print(f"\n cached_after_inv {cached_after_inv}")
        assert cached_after_inv is None

        responce_3 = await client.get(
            f"/api/cache-demo/orders/{o_id}/card",
            params={"use_cache": True}
        )
        assert responce_3.status_code == 200
        data_3 = responce_3.json()
        print(f'\n TOTAL_AMOUNT IS {data_3["total_amount"]}')
        assert data_3["total_amount"] == 250.0

        cached_after_ref = await redis.get(key)
        print(f"\n cached_after_ref {cached_after_ref}")
        assert cached_after_ref is not None

        cached_payload = json.loads(cached_after_ref)
        assert cached_payload["status"] == "created"
        assert cached_payload["total_amount"] == 250.0
        
        await delete_test_order(db_session, o_id, u_id)

    await redis.flushall()
    await redis.aclose()



if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])