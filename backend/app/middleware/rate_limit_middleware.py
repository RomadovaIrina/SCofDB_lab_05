"""Rate limiting middleware template for LAB 05."""

from typing import Callable

from fastapi import Request, Response, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.infrastructure.redis_client import get_redis
from app.infrastructure.cache_keys import payment_rate_limit_key


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Redis-based rate limiting для endpoint оплаты.

    Цель:
    - защита от DDoS/шторма запросов;
    - защита от случайных повторных кликов пользователя.
    """

    def __init__(self, app, limit_per_window: int = 5, window_seconds: int = 10):
      super().__init__(app)
      self.limit_per_window = limit_per_window
      self.window_seconds = window_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
      """
      TODO: Реализовать Redis rate limiting.

        Рекомендуемая логика:
        1) Применять только к endpoint оплаты:
           - /api/orders/{order_id}/pay
           - /api/payments/retry-demo
        2) Сформировать subject:
           - user_id (если есть), иначе client IP.
        3) Использовать Redis INCR + EXPIRE:
           - key = rate_limit:pay:{subject}
           - если counter > limit_per_window -> 429 Too Many Requests.
        4) Для прохождения запроса добавить в ответ headers:
           - X-RateLimit-Limit
           - X-RateLimit-Remaining
        """

      endpont = request.url.path
      if not (
         (endpont.startswith("/api/orders/") and endpont.endswith("/pay"))
         or endpont == "/api/payments/retry-demo"):
         return await call_next(request)
      
      redis = get_redis()

      u_id = request.headers.get("X-User-Id")
      subject = u_id if u_id else (
         request.client.host if request.client else "unknown")

      key = payment_rate_limit_key(subject)

      current = await redis.incr(key)

      if current == 1:
         await redis.expire(key, self.window_seconds)

      left = max(0, self.limit_per_window - current)

      if current >self.limit_per_window:
         return JSONResponse(
            status_code=429,
            content={"detail": "Too many payment requests. Please try again later."},
            headers={
                  "X-RateLimit-Limit": str(self.limit_per_window),
                  "X-RateLimit-Remaining": "0",
               },
         )
      
      response = await call_next(request)
      response.headers["X-RateLimit-Limit"] = str(self.limit_per_window)
      response.headers["X-RateLimit-Remaining"] = str(left)


      return response
         

