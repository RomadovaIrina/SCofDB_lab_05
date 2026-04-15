"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.db import SessionLocal

from sqlalchemy import text


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware для идемпотентности POST-запросов оплаты.

    Идея:
    - Клиент отправляет `Idempotency-Key` в header.
    - Если запрос с таким ключом уже выполнялся для того же endpoint и payload,
      middleware возвращает кэшированный ответ (без повторного списания).
    """

    def __init__(self, app, ttl_seconds: int = 24 * 60 * 60):
        super().__init__(app)
        self.ttl_seconds = ttl_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать алгоритм.

        Рекомендуемая логика:
        1) Пропускать только целевые запросы:
           - method == POST
           - path в whitelist для платежей
        2) Читать Idempotency-Key из headers.
           Если ключа нет -> обычный call_next(request)
        3) Считать request_hash (например sha256 от body).
        4) В транзакции:
           - проверить запись в idempotency_keys
           - если completed и hash совпадает -> вернуть кэш (status_code + body)
           - если key есть, но hash другой -> вернуть 409 Conflict
           - если ключа нет -> создать запись processing
        5) Выполнить downstream request через call_next.
        6) Сохранить response в idempotency_keys со статусом completed.
        7) Вернуть response клиенту.

        Дополнительно:
        - обработайте кейс конкурентных одинаковых ключей
          (уникальный индекс + retry/select existing).
        """

        # Рекомендуемая логика:
        # 1) Пропускать только целевые запросы:
        #    - method == POST
        #    - path в whitelist для платежей

        if request.method != "POST" or \
            not request.url.path.startswith("/api/payments"):
            return await call_next(request)
        
        # 2) Читать Idempotency-Key из headers.
        #    Если ключа нет -> обычный call_next(request)

        idempotency_key = request.headers.get("Idempotency-Key")
        if not idempotency_key:
            return await call_next(request)
        
        # 3) Считать request_hash (например sha256 от body).

        result = await request.body()
        request_hash = self.build_request_hash(result)

        # 4) В транзакции:
        #    - проверить запись в idempotency_keys
        #    - если completed и hash совпадает -> вернуть кэш (status_code + body)
        #    - если key есть, но hash другой -> вернуть 409 Conflict
        #    - если ключа нет -> создать запись processing

        async with SessionLocal() as session:
          async with session.begin():
              
            idempotency_keys_rows = await session.execute(
               text(
                """SELECT id, status, response_body, request_hash, status_code
                    FROM idempotency_keys
                    WHERE idempotency_key = :key 
                    AND request_method =  :method
                    AND request_path = :path
                    FOR UPDATE
                """),
                {
                  "key": idempotency_key,
                  "method": request.method,
                  "path": request.url.path
                  }
            )

            

            row = idempotency_keys_rows.first()

            if row:
              _, status, response_body, saved_request_hash, status_code = row

              if request_hash != saved_request_hash:
                return Response(
                    status_code=409,
                    content=json.dumps({"error": "Conflict: usage of the same key with different payload"}),
                    media_type="application/json",
                )
              if status == "completed":
                return Response(
                    content=json.dumps(response_body) if response_body else "{}",
                    status_code=status_code,
                    headers={"X-Idempotency-Replayed": "true"},
                    media_type="application/json",
                )
              if status == 'processing':
                  return Response(
                      content=json.dumps({"error": "request is already being processed"}),
                      status_code=409,
                      media_type="application/json"
                  )
            else:
                req_text = text("""
                    INSERT INTO idempotency_keys 
                    (idempotency_key, request_method, request_path, request_hash, status, expires_at)
                    VALUES (
                        :key,
                        :method,
                        :path,
                        :request_hash,
                        'processing',
                        NOW() + (:ttl * INTERVAL '1 day')
                    )
                """)

                await session.execute(req_text, {
                    "key": idempotency_key,
                    "method": request.method,
                    "path": request.url.path,
                    "request_hash": request_hash,
                    "ttl": self.ttl_seconds
                })
            
            # 5) Выполнить downstream request через call_next.

          response: Response = await call_next(request)

          response_body = b""
          async for chunk in response.body_iterator:
              response_body += chunk

          json_body = None
          if response_body:
              try:
                  json_body = json.loads(response_body.decode("utf-8"))
              except Exception:
                  json_body = {"raw": response_body.decode("utf-8", errors="replace")}

            # 6) Сохранить response в idempotency_keys со статусом completed.
          async with SessionLocal() as update_session:
              await update_session.execute(
                  text("""
                      UPDATE idempotency_keys
                      SET
                          status = 'completed',
                          status_code = :status_code,
                          response_body = CAST(:response_body AS jsonb),
                          updated_at = NOW()
                        WHERE idempotency_key = :key
                        AND request_method = :method
                        AND request_path = :path
                  """),
                  {
                      "status_code": response.status_code,
                      "response_body": json.dumps(json_body) if json_body else None,
                      "key": idempotency_key,
                      "method": request.method,
                      "path": request.url.path,
                  },
              )
              await update_session.commit()
          # 7) Вернуть response клиенту.  
          return Response(
              content=response_body,
              status_code=response.status_code,
              media_type=response.media_type,
              headers=dict(response.headers),
          )  

        return await call_next(request)

    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса для проверки reuse ключа с другим payload."""
        return hashlib.sha256(raw_body).hexdigest()

    @staticmethod
    def encode_response_payload(body_obj) -> str:
        """Сериализация response body для сохранения в idempotency_keys."""
        return json.dumps(body_obj, ensure_ascii=False)
