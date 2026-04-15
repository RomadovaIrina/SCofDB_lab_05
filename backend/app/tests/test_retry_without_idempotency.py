"""
LAB 04: Проверка идемпотентного повтора запроса.

Цель:
При повторном запросе с тем же Idempotency-Key вернуть
кэшированный результат без повторного списания.
"""

import asyncio
import json
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from httpx import AsyncClient
from app.main import app

from app.application.payment_service import PaymentService

# DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@db:5432/marketplace"

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
    async_session = sessionmaker(
        test_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with async_session() as session:
        yield session


@pytest.fixture
async def test_order(test_engine):

    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    async with AsyncSession(test_engine) as setup_session:
        async with setup_session.begin():

            await setup_session.execute(
                text("""
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:user_id, :email, :name, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "user_id": user_id,
                    "email": f"retry_{order_id}@example.com",
                    "name": "Retry Test User"
                }
            )

            await setup_session.execute(
                text("""
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:order_id, :user_id, 'created', 100.00, NOW())
                """),
                {"order_id": order_id, "user_id": user_id}
            )

            await setup_session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
                {"order_id": order_id}
            )

    yield order_id

    async with AsyncSession(test_engine) as cleanup_session:
        async with cleanup_session.begin():

            await cleanup_session.execute(
                text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                {"order_id": order_id}
            )

            await cleanup_session.execute(
                text("DELETE FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )

            await cleanup_session.execute(
                text("DELETE FROM users WHERE id = :user_id"),
                {"user_id": user_id}
            )



@pytest.mark.asyncio
async def test_retry_without_idempotency_can_double_pay(test_order, test_engine):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Выполнить две параллельные попытки POST /api/payments/retry-demo
       с mode='unsafe' и БЕЗ заголовка Idempotency-Key.
    3) Проверить историю order_status_history:
       - paid-событий больше 1 (или иная метрика двойного списания).
    4) Вывести понятный отчёт в stdout:
       - сколько попыток
       - сколько paid в истории
       - почему это проблема.
    """
    order_id = test_order

    async def retry_payment_1():
        async with AsyncSession(test_engine) as session:
            service = PaymentService(session)
            await service.pay_order_unsafe(order_id)
    
    result = await asyncio.gather(
        retry_payment_1(),
        retry_payment_1(),
        return_exceptions=True
    )
    print("\nRESULTS\n")
    print(result)

    await asyncio.sleep(1)  # это чтобы все завершилось 

    async with AsyncSession(test_engine) as session:
        service = PaymentService(session)
        history = await service.get_payment_history(order_id)
    
    for h in history:
        print(h)


    assert len(history) == 2, "Ждали 2 оплаты = race condition"

