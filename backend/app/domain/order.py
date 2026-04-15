"""Доменные сущности заказа."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List

from dataclasses import dataclass, field

from .exceptions import (
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError,
)


# TODO: Реализовать OrderStatus (str, Enum)
# Значения: CREATED, PAID, CANCELLED, SHIPPED, COMPLETED
class OrderStatus(str, Enum):
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"
    SHIPPED = "shipped"
    COMPLETED = "completed"


# TODO: Реализовать OrderItem (dataclass)
# Поля: product_name, price, quantity, id, order_id
# Свойство: subtotal (price * quantity)
# Валидация: quantity > 0, price >= 0
@dataclass
class OrderItem:
    product_name: str
    price: Decimal
    quantity: int
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    order_id: uuid.UUID = field(default_factory=uuid.uuid4)

    @property
    def subtotal(self) -> Decimal:
        return self.price * Decimal(self.quantity)
    
    def __post_init__(self):
        if self.quantity <= 0:
            raise InvalidQuantityError(self.quantity)
        if self.price < 0:
            raise InvalidPriceError(self.price)


# TODO: Реализовать OrderStatusChange (dataclass)
# Поля: order_id, status, changed_at, id
@dataclass
class OrderStatusChange:
    order_id: uuid.UUID
    status: OrderStatus
    changed_at: datetime = field(default_factory=datetime.now)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


# TODO: Реализовать Order (dataclass)
# Поля: user_id, id, status, total_amount, created_at, items, status_history
# Методы:
#   - add_item(product_name, price, quantity) -> OrderItem
#   - pay() -> None  [КРИТИЧНО: нельзя оплатить дважды!]
#   - cancel() -> None
#   - ship() -> None
#   - complete() -> None
@dataclass
class Order:
    user_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: OrderStatus = field(default=OrderStatus.CREATED)
    total_amount: Decimal = field(default=Decimal(0))
    created_at: datetime = field(default_factory=datetime.now)
    items: List[OrderItem] = field(default_factory=list)
    status_history: List[OrderStatusChange] = field(default_factory=list)

    def __post_init__(self):
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=self.status))
    
    def _renew_history(self, new_status: OrderStatus):
        renew_status = OrderStatusChange(order_id=self.id, status=new_status)
        self.status_history.append(renew_status)
    
    def add_item(self, product_name: str, price: Decimal, quantity: int) -> OrderItem:
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        
        item = OrderItem(product_name=product_name, price=price, quantity=quantity)
        item.order_id = self.id
        self.items.append(item)
        self.total_amount += item.subtotal

        if self.total_amount < 0:
            raise InvalidAmountError(self.total_amount)
        
        return item
    
    def pay(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)

        self.status = OrderStatus.PAID
        self._renew_history(self.status)
    
    def cancel(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        
        if self.status == OrderStatus.SHIPPED:
            raise ValueError("Already shipped order cannot be cancelled")
        
        if self.status == OrderStatus.COMPLETED:
            raise ValueError("Already completed order cannot be cancelled")
        
        self.status = OrderStatus.CANCELLED
        self._renew_history(self.status)

    def ship(self) -> None:
        if self.status != OrderStatus.PAID:
            raise ValueError(self.id)
        self.status = OrderStatus.SHIPPED
        self._renew_history(self.status)

    def complete(self) -> None:
        if self.status != OrderStatus.SHIPPED:
            raise ValueError(self.id)
        self.status = OrderStatus.COMPLETED
        self._renew_history(self.status)