"""
Тест для демонстрации РЕШЕНИЯ проблемы race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_safe() заказ оплачивается только один раз.
"""

import asyncio
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
# этих не было
from sqlalchemy import text
import time

from app.application.payment_service import PaymentService
from app.domain.exceptions import OrderAlreadyPaidError


# TODO: Настроить подключение к тестовой БД
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def db_session():
    """
    Создать сессию БД для тестов.
    
    TODO: Реализовать фикстуру (см. test_concurrent_payment_unsafe.py)
    """
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session
    await engine.dispose()

@pytest.fixture
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    
    TODO: Реализовать фикстуру (см. test_concurrent_payment_unsafe.py)
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
                """),
            dummy_user
        )

        await db_session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, NOW())
                """),
            dummy_order
        )

        await db_session.execute(
            text(
                """
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
            {"order_id": order_id}
        )  

    await db_session.commit()

    yield order_id

    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :order_id"),
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
async def test_concurrent_payment_safe_prevents_race_condition(db_session, test_order):
    """
    Тест демонстрирует решение проблемы race condition с помощью pay_order_safe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен только один раз.
    Это показывает, что метод pay_order_safe() защищен от конкурентных запросов.
    
    TODO: Реализовать тест следующим образом:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_safe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_safe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_safe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить результаты:
       - Одна попытка должна УСПЕШНО завершиться
       - Вторая попытка должна выбросить OrderAlreadyPaidError ИЛИ вернуть ошибку
       
       success_count = sum(1 for r in results if not isinstance(r, Exception))
       error_count = sum(1 for r in results if isinstance(r, Exception))
       
       assert success_count == 1, "Ожидалась одна успешная оплата"
       assert error_count == 1, "Ожидалась одна неудачная попытка"
       
    4. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
       assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
       
    5. Вывести информацию об успешном решении:
       
       print(f"✅ RACE CONDITION PREVENTED!")
       print(f"Order {order_id} was paid only ONCE:")
       print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
       print(f"Second attempt was rejected: {results[1]}")
    """
    order_id = test_order

    engine_1 = create_async_engine(DATABASE_URL)
    engine_2 = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        async with AsyncSession(engine_1) as session1:
            service1 = PaymentService(session1)
            return await service1.pay_order_safe(order_id)

    async def payment_attempt_2():
        async with AsyncSession(engine_2) as session2:
            service2 = PaymentService(session2)
            return await service2.pay_order_safe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    success_att = sum(1 for r in results if not isinstance(r, Exception))
    error_att = sum(1 for r in results if isinstance(r, Exception))

    assert success_att == 1, f"Ожидалась одна успешная оплата"
    assert error_att == 1, "Ожидалась одна неудачная попытка"

    service = PaymentService(db_session)
    history = await service.get_payment_history(order_id)
    
    # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
    assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
    
    print(f"✅ RACE CONDITION PREVENTED!")
    print(f"Order {order_id} was paid only ONCE:")
    print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
    print(f"Second attempt was rejected: {results[1]}")

    await engine_1.dispose()
    await engine_2.dispose()


@pytest.mark.asyncio
async def test_concurrent_payment_safe_with_explicit_timing(db_session, test_order):
    """
    Дополнительный тест: проверить работу блокировок с явной задержкой.
    
    TODO: Реализовать тест с добавлением задержки в первой транзакции:
    
    1. Первая транзакция:
       - Начать транзакцию
       - Заблокировать заказ (FOR UPDATE)
       - Добавить задержку (asyncio.sleep(1))
       - Оплатить
       - Commit
       
    2. Вторая транзакция (запустить через 0.1 секунды после первой):
       - Начать транзакцию
       - Попытаться заблокировать заказ (FOR UPDATE)
       - ДОЛЖНА ЖДАТЬ освобождения блокировки от первой транзакции
       - После освобождения - увидеть обновленный статус 'paid'
       - Выбросить OrderAlreadyPaidError
       
    3. Проверить временные метки:
       - Вторая транзакция должна завершиться ПОЗЖЕ первой
       - Разница должна быть >= 1 секунды (время задержки)
       
    Это подтверждает, что FOR UPDATE действительно блокирует строку.
    """

    order_id = test_order

    engine_1 = create_async_engine(DATABASE_URL)
    engine_2 = create_async_engine(DATABASE_URL)

    timies = {}

    async def payment_attempt_1():
        async with AsyncSession(engine_1) as session1:
            await session1.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )

            # фор апдейт нужен для того чтобы блокироваться а не для того чтоб блокировать
            # типа, если параллеьно идет коммит внутри которого есть желание записать ресурс который нам нужен
            # мы ждем конца и если оно успешно мы умираем 
            result = await session1.execute(
                text("""
                    SELECT status FROM orders
                    WHERE id = :order_id
                    FOR UPDATE
                """),
                {"order_id": str(order_id)}
            )

            timies['attempt_1_before_sleep'] = time.time()

            await asyncio.sleep(1) 

            await session1.execute(
                text("""
                    UPDATE orders SET status = 'paid'
                    WHERE id = :order_id AND status = 'created'
                """),
                {"order_id": str(order_id)}
            )

            await session1.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
                """),
                {"order_id": str(order_id)}
            )

            await session1.commit()
            timies['attempt_1_finish'] = time.time()
            return {"status": "paid", "attempt": "1"}
        
    async def payment_attempt_2():
        await asyncio.sleep(0.1) 
        async with AsyncSession(engine_2) as session2:
            await session2.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )

            timies['attempt_2_start'] = time.time()

            result = await session2.execute(
                text("""
                    SELECT status FROM orders
                    WHERE id = :order_id
                    FOR UPDATE
                """),
                {"order_id": str(order_id)}
            )

            timies['attempt_2_after_lock'] = time.time()

            row = result.first()
            assert row is not None, "Заказ не найден во второй транзакции"

            if row[0] != 'created':
                raise OrderAlreadyPaidError(f"Order with ID {order_id} already paid")

            await session2.execute(
                text("""
                     UPDATE orders SET status = 'paid'
                     WHERE id = :order_id AND status = 'created'
                """),
                {"order_id": str(order_id)}
            )

            await session2.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
                """),
                {"order_id": str(order_id)}
            )

            await session2.commit()
            timies['attempt_2_finish'] = time.time()
            return {"status": "paid", "attempt": "2"}

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )


    success_att = sum(1 for r in results if not isinstance(r, Exception))
    error_att = sum(1 for r in results if isinstance(r, Exception))

    if 'attempt_1_finish' in timies and 'attempt_2_start' in timies:
        print(f"Attempt 1 finished at: {timies['attempt_1_finish']}")
        print(f"Attempt 2 started at: {timies['attempt_2_start']}") 

    assert success_att == 1, f"Ожидалась одна успешная оплата"
    assert error_att == 1, f"Ожидалась одна неудачная попытка"

    history = await PaymentService(db_session).get_payment_history(order_id)

    assert len(history) == 1, "Ожидалась 1 запись об оплате"

    await engine_1.dispose()
    await engine_2.dispose()


@pytest.fixture
async def test_orders(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    
    TODO: Реализовать фикстуру (см. test_concurrent_payment_unsafe.py)
    """
    user_id = uuid.uuid4()
    order_id_1 = uuid.uuid4()
    order_id_2 = uuid.uuid4()


    dummy_user = {
        "id": user_id,
        "email": f"dummy_user{str(user_id)[:8]}@dummy.com",
        "name": "Dummy User",
    }

    dummy_order_1 = {
        "id": order_id_1,
        "user_id": user_id,
        "status": "created",
        "total_amount": 100.0,
    }

    dummy_order_2 = {
        "id": order_id_2,
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
                """),
            dummy_user
        )

        await db_session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, NOW())
                """),
            dummy_order_1
        )

        await db_session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, NOW())
                """),
            dummy_order_2
        )

        

        await db_session.execute(
            text(
                """
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
            {"order_id": order_id_1}
        )  

        await db_session.execute(
            text(
                """
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
            {"order_id": order_id_2}
         )

    await db_session.commit()

    yield order_id_1, order_id_2

    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id IN (:order_id_1, :order_id_2)"),
        {"order_id_1": order_id_1, "order_id_2": order_id_2}
    )
    await db_session.execute(
        text("DELETE FROM order_items WHERE order_id IN (:order_id_1, :order_id_2)"),
        {"order_id_1": order_id_1, "order_id_2": order_id_2}
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id IN (:order_id_1, :order_id_2)"),
        {"order_id_1": order_id_1, "order_id_2": order_id_2}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :user_id"),
        {"user_id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_concurrent_payment_safe_multiple_orders(db_session, test_orders):
    """
    Дополнительный тест: проверить, что блокировки не мешают разным заказам.
    
    TODO: Реализовать тест:
    1. Создать ДВА разных заказа
    2. Оплатить их ПАРАЛЛЕЛЬНО с помощью pay_order_safe()
    3. Проверить, что ОБА успешно оплачены
    
    Это показывает, что FOR UPDATE блокирует только конкретную строку,
    а не всю таблицу, что важно для производительности.
    """
    order_id_1, order_id_2 = test_orders

    engine_1 = create_async_engine(DATABASE_URL)
    engine_2 = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        async with AsyncSession(engine_1) as session1:
            service1 = PaymentService(session1)
            return await service1.pay_order_safe(order_id_1)
        
    async def payment_attempt_2():
        async with AsyncSession(engine_2) as session2:
            service2 = PaymentService(session2)
            return await service2.pay_order_safe(order_id_2)
        
    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    success_att = sum(1 for r in results if not isinstance(r, Exception))
    error_att = sum(1 for r in results if isinstance(r, Exception))

    print("Order 1 result:", {"goood": results[0]} if not isinstance(results[0], Exception) else f"Error: {results[0]}")
    print("Order 2 result:", {"goood": results[1]} if not isinstance(results[1], Exception) else f"Error: {results[1]}")

    await engine_1.dispose()
    await engine_2.dispose()

    assert success_att == 2, f"Ожидалось две успешные оплаты"
    assert error_att == 0, f"Ожидалось отсутствие ошибок при оплате"



if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_safe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_safe_prevents_race_condition PASSED
    
    Вывод должен показывать:
    ✅ RACE CONDITION PREVENTED!
    Order XXX was paid only ONCE:
      - 2024-XX-XX: status = paid
    Second attempt was rejected: OrderAlreadyPaidError(...)
    """
    pytest.main([__file__, "-v", "-s"])
