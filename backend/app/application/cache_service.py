"""Cache service template for LAB 05."""

import json
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.redis_client import get_redis
from app.infrastructure.cache_keys import catalog_key, order_card_key


class CacheService:
    """
    Сервис кэширования каталога и карточки заказа.

    TODO:
    - реализовать методы через Redis client + БД;
    - добавить TTL и версионирование ключей.
    """
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.redis = get_redis()
        self.ttl_seconds = 300

    async def get_catalog(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть catalog из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """

        key = catalog_key()

        if (use_cache):
            data_cached = await self.redis.get(key)
            if data_cached:
                return json.loads(data_cached)
            
        result = await self.db.execute(text("""
            SELECT
                oi.product_name,
                count(*) AS order_lines,
                sum(oi.quantity) AS sold_qty,
                round(avg(oi.price)::numeric, 2) AS avg_price
            FROM order_items oi
            GROUP BY oi.product_name
            ORDER BY sold_qty DESC
            LIMIT 100 
        """))

        rows  = result.fetchall()

        catalog = []
        for row in rows:
            catalog.append({
                "product_name": row[0],
                "order_lines": row[1],
                "sold_qty": row[2],
                "avg_price": float(row[3]) if row[3] else 0,
            })

        if use_cache:
            await self.redis.setex(
                key,
                self.ttl_seconds,
                json.dumps(catalog, default=str),
            )
        
        return catalog


    async def get_order_card(self, order_id: str, *, use_cache: bool = True) -> Optional[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть карточку заказа из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        key = order_card_key(order_id)

        if use_cache:
            data_cached = await self.redis.get(key)
            if data_cached:
                return json.loads(data_cached)
        
        result = await self.db.execute(
            text("""
                SELECT id, user_id, 
                 status, total_amount, created_at
                FROM orders WHERE id = :order_id
            """),
            {"order_id": order_id},
        )
        row = result.fetchone()
        if not row:
            return None
        
        order_result = await self.db.execute(text("""
            SELECT
                o.id, o.user_id, o.status,
                o.total_amount, o.created_at,
                u.email, u.name
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.id = :order_id
        
        """), 
        {
            "order_id": order_id
        })

        order_row = order_result.fetchone()
        if not order_row:
            return None
        
        items_result = await self.db.execute(text("""
            SELECT id, product_name,
            price, quantity
            FROM order_items
            WHERE order_id = :order_id
        
        """), 
        {
            "order_id": order_id
        })

        items = []

        for item in items_result:
            items.append({
                "id": str(item[0]),
                "product_name": item[1],
                "price": float(item[2]),
                "quantity": item[3],
                "subtotal": float(item[2] * item[3]),
            })
        order_data = {
            "id": str(order_row[0]),
            "user_id": str(order_row[1]),
            "status": order_row[2],
            "total_amount": float(order_row[3]),
            "created_at": order_row[4].isoformat() if order_row[4] else None,
            "user": {
                "email": order_row[5],
                "name": order_row[6],
            },
            "items": items,
        }

        if use_cache:
            await self.redis.setex(
                key,
                self.ttl_seconds,
                json.dumps(order_data, default=str),
            )

        return order_data

        

    async def invalidate_order_card(self, order_id: str) -> None:
        """TODO: Удалить ключ карточки заказа из Redis."""
        cache_key = order_card_key(order_id)
        await self.redis.delete(cache_key)

    async def invalidate_catalog(self) -> None:
        """TODO: Удалить ключ каталога из Redis."""
        cache_key = catalog_key()
        await self.redis.delete(cache_key)
