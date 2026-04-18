"""
LAB 05: Rate limiting endpoint оплаты через Redis.
"""

import uuid

import pytest
from httpx import AsyncClient, ASGITransport
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.main import app


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
            "email": f"rate_{str(user_id)[:8]}@dummy.com",
            "name": "Rate Limit User",
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
async def test_payment_endpoint_rate_limit(test_engine, db_session):
    """
    TODO: Реализовать тест.

    Рекомендуемая проверка:
    1) Сделать N запросов оплаты в пределах одного окна.
    2) Проверить, что первые <= limit проходят.
    3) Следующие запросы получают 429 Too Many Requests.
    4) Проверить заголовки X-RateLimit-Limit / X-RateLimit-Remaining.
    """
    limit = 5

    ordrs = []

    for _ in range(limit + 1):
        o_id, u_id = await create_test_order(db_session)
        ordrs.append((o_id, u_id))
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        header = {"X-User-Id": "rate-limit-test-user"}
        # делаем запросы в пределах лимита запросов
        for i  in range(limit):
            responce  = await client.post(
                    f"/api/orders/{ordrs[i][0]}/pay",
                    headers=header,
                )
            assert responce.status_code == 200
            assert responce.headers["X-RateLimit-Limit"] == str(limit)
            assert responce.headers["X-RateLimit-Remaining"] == str(limit - i - 1)
        
        # а вот теперь делаем "лишний запрос", чтобы увидить ошибку
        responce  = await client.post(
                    f"/api/orders/{ordrs[limit][0]}/pay",
                    headers=header,
                )
        assert responce.status_code == 429 # финиш
        assert responce.headers["X-RateLimit-Limit"] == str(limit)
        assert responce.headers["X-RateLimit-Remaining"] == str(0)

        redis = Redis.from_url(REDIS_URL, decode_responses=True)
        await redis.flushall()

        for o_id, u_id in ordrs:
            await delete_test_order(db_session, o_id, u_id)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
