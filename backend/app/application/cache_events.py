"""Event-driven cache invalidation template for LAB 05."""

from dataclasses import dataclass


@dataclass
class OrderUpdatedEvent:
    """Событие изменения заказа."""

    order_id: str


class CacheInvalidationEventBus:
    """
    Минимальный event bus для LAB 05.

    TODO:
    - реализовать publish/subscribe;
    - на OrderUpdatedEvent инвалидировать:
      - order_card:v1:{order_id}
      - catalog:v1 (если изменение затрагивает агрегаты каталога).
    """
    
    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    async def publish_order_updated(self, event: OrderUpdatedEvent) -> None:
        order_card_key = f"order_card:v1:{event.order_id}"
        catalog_key = "catalog:v1"
        
        await self.redis.delete(order_card_key, catalog_key)