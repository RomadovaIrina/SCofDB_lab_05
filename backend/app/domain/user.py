"""Доменная сущность пользователя."""

import uuid
from datetime import datetime


from dataclasses import dataclass, field
import re


from .exceptions import InvalidEmailError


# TODO: Реализовать класс User
# - Использовать @dataclass
# - Поля: email, name, id, created_at
# - Реализовать валидацию email в __post_init__
# - Regex: r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

EMAIL__VAL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

@dataclass
class User:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    email: str = field(default="")
    name: str = field(default="")
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not re.match(EMAIL__VAL_REGEX, self.email):
            raise InvalidEmailError(f"Invalid email: {self.email}")