"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)
"""

import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
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
    Session = sessionmaker(
        test_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    async with Session() as session:
        yield session


@pytest.fixture
async def test_order(db_session):
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    await db_session.execute(
        text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, NOW())
        """),
        {
            "id": user_id,
            "email": f"compare_{str(user_id)[:8]}@dummy.com",
            "name": "Compare User",
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

    yield order_id

    await db_session.execute(
        text("DELETE FROM idempotency_keys WHERE request_path = '/api/payments/retry-demo'")
    )
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


@pytest.fixture
async def second_test_order(db_session):
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    await db_session.execute(
        text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, NOW())
        """),
        {
            "id": user_id,
            "email": f"compare2_{str(user_id)[:8]}@dummy.com",
            "name": "Compare User 2",
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

    yield order_id

    await db_session.execute(
        text("DELETE FROM idempotency_keys WHERE request_path = '/api/payments/retry-demo'")
    )
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
async def test_compare_for_update_and_idempotency_behaviour(test_order, second_test_order, db_session, test_engine):
   """
   TODO: Реализовать сравнительный тест/сценарий.

   Минимум сравнения:
   1) Повтор запроса с mode='for_update':
      - защита от гонки на уровне БД,
      - повтор может вернуть бизнес-ошибку \"already paid\".
   2) Повтор запроса с mode='unsafe' + Idempotency-Key:
      - второй вызов возвращает тот же кэшированный успешный ответ,
      без повторного списания.

   В конце добавьте вывод:
   - чем отличаются цели и UX двух подходов,
   - почему они не взаимоисключающие и могут использоваться вместе.
   """

   lab_2_for_upd_order = test_order
   lab_4_idkey = second_test_order

   idempotency_key = "fixed-key-123"


   async with AsyncClient(app = app, base_url="http://test") as test_client:
      
      print("Lab 02 FOR UPDATE")

      lab_2_payload = {
          "order_id": str(lab_2_for_upd_order),
          "mode": "for_update"
      }

      response_lab_2_1 = await test_client.post(
          "/api/payments/retry-demo",
          json = lab_2_payload
      )

      data_1 = response_lab_2_1.json()

      assert response_lab_2_1.status_code == 200

      response_lab_2_2 = await test_client.post(
          "/api/payments/retry-demo",
          json = lab_2_payload
      )

      data_2 = response_lab_2_2.json()

      assert response_lab_2_2.status_code == 200
      assert data_2.get("success") is False

      print(f"Первый ответ: {response_lab_2_1.status_code}, {data_1}")
      print(f"Повторный ответ : {response_lab_2_2.status_code}, {data_2}")


      print("Lab 04 Idempotency key")

      payload = {
          "order_id": str(lab_4_idkey),
          "mode": "unsafe"
      }

      
      headers = {
         "Idempotency-Key": idempotency_key
      }

      response_lab_4_1 = await test_client.post(
         "/api/payments/retry-demo",
         json=payload,
         headers=headers
      )

      assert response_lab_4_1.status_code == 200

      response_lab_4_2 = await test_client.post(
         "/api/payments/retry-demo",
         json=payload,
         headers=headers
      )

      assert response_lab_4_2.status_code == 200
      assert response_lab_4_2.headers.get("X-Idempotency-Replayed") == "true"
      assert response_lab_4_1.json() == response_lab_4_2.json()

      print(f"Первый ответ : {response_lab_4_1.status_code}, {response_lab_4_1.json()}")
      print(f"Повторный ответ : {response_lab_4_2.status_code}, {response_lab_4_2.json()}")

   async with AsyncSession(test_engine) as session:
       servise = PaymentService(session)

       histroty_lab_2 = await servise.get_payment_history(lab_2_for_upd_order)
       historty_lab_4 = await servise.get_payment_history(lab_4_idkey)

   paid_count_lab_2 = sum(1 for row in histroty_lab_2 if row["status"] == "paid")
   paid_count_lab_4 = sum(1 for row in historty_lab_4 if row["status"] == "paid")

   assert paid_count_lab_2 == 1
   assert paid_count_lab_4 == 1


   print("\nChfdytybt")

   for_upd = """

   решение из второй лабораторной помогает бороться с race_condition на уровне БД
   for update блокирует строку, поэтому второй запрос видит оплату заказа
   но статус будет 200 хотя попытка неуспешна
   
   """

   idkey = """

   защита на другом уровне = на уровне например сетевых проблем
   происходит кэширование ответа первого запроса И
   И второй запрос пройдет без повторной оплаты
   """

   print(for_upd)

   print(idkey)
