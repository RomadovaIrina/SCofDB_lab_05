"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    """Репозиторий для User."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(user: User) -> None
    # Используйте INSERT ... ON CONFLICT DO UPDATE
    async def save(self, user: User) -> None:
        bd_query = text(
        """
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                email = EXCLUDED.email,
                name = EXCLUDED.name
        """)

        await self.session.execute(bd_query, 
            {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "created_at": user.created_at 
            })
        # 
        await self.session.flush()
    # TODO: Реализовать find_by_id(user_id: UUID) -> Optional[User]
    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        bd_query = text(
        """
            SELECT id, email, name, created_at
            FROM users
            WHERE id = :id
        """)

        result = await self.session.execute(bd_query, 
            {"id": str(user_id)})
        
        row = result.first()

        if row is None:
            return None
        return User(
                id=row[0],
                email=row[1],
                name=row[2],
                created_at=row[3]
            )

    # TODO: Реализовать find_by_email(email: str) -> Optional[User]
    async def find_by_email(self, email: str) -> Optional[User]:
        bd_query = text(
        """
            SELECT id, email, name, created_at
            FROM users
            WHERE email = :email
        """)

        result = await self.session.execute(bd_query, 
            {"email": email})
        
        row = result.first()

        if row is None:
            return None
        return User(
                id=row[0],
                email=row[1],
                name=row[2],
                created_at=row[3]
            )

    # TODO: Реализовать find_all() -> List[User]
    async def find_all(self) -> List[User]:
        bd_query = text(
        """
            SELECT id, email, name, created_at
            FROM users
        """)

        result = await self.session.execute(bd_query)
        
        rows = result.fetchall()

        users = []
        for row in rows:
            users.append(User(
                id=row[0],
                email=row[1],
                name=row[2],
                created_at=row[3]
            ))
        return users


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(order: Order) -> None
    # Сохранить заказ, товары и историю статусов
    async def save(self, order: Order) -> None:
        bd_query = text(
        """
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, :status, :total_amount, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount
        """)
        await self.session.execute(bd_query, 
            {
            "id": order.id,
            "user_id": order.user_id,
            "status": order.status.value,
            "total_amount": order.total_amount,
            "created_at": order.created_at 
            })
        
        # убиваем то, что пользователь убил сам
        delete_order_items_query = text(
            """
            DELETE FROM order_items WHERE order_id = :order_id
            """)
        await self.session.execute(delete_order_items_query, {"order_id": order.id})
        

        if order.items:
            for item in order.items:
                insert_item_query = text("""
                    INSERT INTO order_items (id, order_id, product_name, price, quantity)
                    VALUES (:id, :order_id, :product_name, :price, :quantity)
                """)
                await self.session.execute(
                    insert_item_query,
                    {
                        "id": item.id,
                        "order_id": order.id,
                        "product_name": item.product_name,
                        "price": item.price,
                        "quantity": item.quantity,
                    }
                )
        
        if order.status_history:
            delete_history_query = text("""
                DELETE FROM order_status_history WHERE order_id = :order_id
            """)
            await self.session.execute(delete_history_query, {"order_id": order.id})
            
            for history in order.status_history:
                history_query = text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, :status, :changed_at)
                """)
                await self.session.execute(
                    history_query,
                    {
                        "id": history.id,
                        "order_id": order.id,
                        "status": history.status.value ,
                        "changed_at": history.changed_at,
                    }
                )
        
        await self.session.flush()
                

    # TODO: Реализовать find_by_id(order_id: UUID) -> Optional[Order]
    # Загрузить заказ со всеми товарами и историей
    # Используйте object.__new__(Order) чтобы избежать __post_init__
    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        bd_order_query = text(
        """
            SELECT id, user_id, status, total_amount, created_at
            FROM orders
            WHERE id = :id
        """)
        result = await self.session.execute(bd_order_query, {"id": order_id})
        row = result.first()
        if not row:
            return None
        
        order = object.__new__(Order)
        order.id = row[0]
        order.user_id = row[1]
        order.created_at = row[4]
        order.status = OrderStatus(row[2])
        order.total_amount = Decimal(str(row[3]))
        order.items = []
        order.status_history = []

        bd_items_query = text(
        """
            SELECT id, product_name, price, quantity
            FROM order_items
            WHERE order_id = :order_id
        """)
        items_result = await self.session.execute(bd_items_query, {"order_id": order_id})
        for item_row in items_result.fetchall():

            item = object.__new__(OrderItem)
            item.id = item_row[0]
            item.product_name = item_row[1]
            item.price = Decimal(str(item_row[2]))
            item.quantity = item_row[3]
            order.items.append(item)

        bd_history_query = text(
        """
            SELECT id, status, changed_at
            FROM order_status_history
            WHERE order_id = :order_id
            ORDER BY changed_at ASC
        """)

        history_result = await self.session.execute(bd_history_query, {"order_id": order_id})
        for history_row in history_result:

            history = object.__new__(OrderStatusChange)
            history.id = history_row[0]
            history.status = OrderStatus(history_row[1])
            history.changed_at = history_row[2]
            order.status_history.append(history)

        return order


    # TODO: Реализовать find_by_user(user_id: UUID) -> List[Order]
    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        bd_query = text(
        """
            SELECT id
            FROM orders
            WHERE user_id = :user_id
        """)
        result = await self.session.execute(bd_query, {"user_id": user_id})
        orders = []

        for row in result:
            order_id = row[0]
            order = await self.find_by_id(order_id)
            if order:
                orders.append(order)
        
        return orders
    
    # TODO: Реализовать find_all() -> List[Order]
    async def find_all(self) -> List[Order]:
        bd_query = text(
        """
            SELECT id
            FROM orders
        """)
        result = await self.session.execute(bd_query)
        orders = []

        for row in result:
            order_id = row[0]
            order = await self.find_by_id(order_id)
            if order:
                orders.append(order)
        
        return orders