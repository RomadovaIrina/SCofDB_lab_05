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
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.

    """
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    dummy_user = {
        "id": user_id,
        "email": f"dummy_user{str(user_id)[:8]}@dummy.com",
        "name": "Dummy User",
    }

    dummy_order = {
        "id": order_id,
        "user_id": user_id,
        "status": "created",
        "total_amount": 100.0,
    }

    async with db_session.begin():
        await db_session.execute(
            text(
                """
                INSERT INTO users (id, email, name, created_at)
                VALUES (:id, :email, :name, NOW())
                """
            ),
            dummy_user,
        )

        await db_session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, NOW())
                """
            ),
            dummy_order,
        )

        await db_session.execute(
            text(
                """
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """
            ),
            {"order_id": order_id},
        )

    await db_session.commit()

    yield order_id

    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :order_id"),
        {"order_id": order_id},
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id = :order_id"), {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :user_id"), {"user_id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_retry_with_same_key_returns_cached_response(test_order, test_engine):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Сделать первый POST /api/payments/retry-demo (mode='unsafe')
       с заголовком Idempotency-Key: fixed-key-123.
    3) Повторить тот же POST с тем же ключом и тем же payload.
    4) Проверить:
       - второй ответ пришёл из кэша (через признак, который вы добавите,
       например header X-Idempotency-Replayed=true),
       - в order_status_history только одно событие paid,
       - в idempotency_keys есть запись completed с response_body/status_code.
    """
    order_id = test_order

    key = "fixed-key-123"
    mode = "unsafe"

    async with AsyncClient(app=app, base_url="http://test") as test_client:
        headers = {"Idempotency-Key": key}
        payload = {"order_id": str(order_id), "mode": mode}

        response_1 = await test_client.post(
            "/api/payments/retry-demo", json=payload, headers=headers
        )

        print(f" response_1: {json.loads(response_1.content)}")

        responce_status_1 = response_1.status_code
        assert responce_status_1 == 200

        response_2 = await test_client.post(
            "/api/payments/retry-demo", json=payload, headers=headers
        )

        print(f" response_2: {json.loads(response_2.content)}")

        responce_status_2 = response_2.status_code
        assert responce_status_2 == 200

        assert response_2.headers.get("X-Idempotency-Replayed") == "true"

        assert response_1.json() == response_2.json()

        async with AsyncSession(test_engine) as session:
            service = PaymentService(session)
            history = await service.get_payment_history(order_id)

        paid_events = [row for row in history if row["status"] == "paid"]

        # assert len(history) == 1, "Ждали 1 оплату"

        assert len(paid_events) == 1, "Ждали 1 событие paid в истории"

        async with AsyncSession(test_engine) as idkey_session:
            idk_records = await idkey_session.execute(
                text(
                    """
            SELECT status, response_body, status_code
            FROM idempotency_keys
            WHERE idempotency_key = :key"""
                ),
                {"key": key}
            )
            idk_record = idk_records.first()
            assert idk_record is not None, "Ожидали запись в idempotency_keys"
            status, response_body, status_code = idk_record
            assert status == "completed", "Ожидали статус completed"

        async with AsyncSession(test_engine) as idk_cleanup_session:
            await idk_cleanup_session.execute(
                text(
                    """
            DELETE FROM idempotency_keys
            WHERE idempotency_key = :key"""
                ),
                {"key": key}
            )
            await idk_cleanup_session.commit()


@pytest.mark.asyncio
async def test_same_key_different_payload_returns_conflict(test_order, test_engine):
    """
    TODO: Реализовать негативный тест.

    Один и тот же Idempotency-Key нельзя использовать с другим payload.
    Ожидается 409 Conflict (или эквивалентная бизнес-ошибка).
    """
    order_id = test_order
    
    key = "fixed-key-123"
    mode_1 = "unsafe"
    mode_2 = "safe"

    async with AsyncClient(app=app, base_url="http://test") as test_client:
        headers = {"Idempotency-Key": key}
        payload_1 = {"order_id": str(order_id), "mode": mode_1}
        payload_2 = {"order_id": str(order_id), "mode": mode_2}


        response_1 = await test_client.post(
            "/api/payments/retry-demo",
            json=payload_1,
            headers=headers
        )

        print(f"\n RESPONSE 1: {json.loads(response_1.content)}")

        responce_status_1 = response_1.status_code
        assert responce_status_1 == 200

        payload_2 = {"order_id": str(order_id), "mode": "for_update"}

        response_2 = await test_client.post(
            "/api/payments/retry-demo",
            json=payload_2,
            headers=headers
        )

        print(f"\n RESPONSE 2: {json.loads(response_2.content)}")

        responce_status_2 = response_2.status_code
        assert responce_status_2 == 409, "Ожидали 409 при повторном использовании ключа с другим payload"


        async with AsyncSession(test_engine) as idk_cleanup_session:
            await idk_cleanup_session.execute(
                text(
                    """
            DELETE FROM idempotency_keys
            WHERE idempotency_key = :key"""
                ),
                {"key": key}
            )
            await idk_cleanup_session.commit()

