"""Replaceable scoped authentication and credential-safe diagnostics."""

import hmac
import json
import logging
import re
from collections.abc import Mapping
from typing import Protocol

from fastapi import HTTPException

from wood_events_service.config import ProducerCredential, Scope

SENSITIVE_KEY = re.compile(
    r"password|secret|credential|authorization|token|api[_-]?key|database[_-]?url",
    re.IGNORECASE,
)


class ServiceAuth(Protocol):
    def authenticate(self, token: str, scope: Scope) -> str: ...


class TokenAuth:
    def __init__(self, credentials: list[ProducerCredential]) -> None:
        self._credentials = credentials

    def authenticate(self, token: str, scope: Scope) -> str:
        match: ProducerCredential | None = None
        for credential in self._credentials:
            if hmac.compare_digest(
                token.encode(), credential.token.get_secret_value().encode()
            ):
                match = credential
        if match is None:
            raise HTTPException(
                401, "Authentication required", headers={"WWW-Authenticate": "Bearer"}
            )
        if scope not in match.scopes:
            raise HTTPException(403, "Credential lacks required scope")
        return match.source


class SecretPolicy:
    def __init__(self, values: tuple[str, ...]) -> None:
        self._values = tuple(value for value in values if value)

    def redact(self, value: str) -> str:
        for secret in sorted(self._values, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
        return value

    def ensure_safe(self, value: object) -> None:
        """Fail closed for credential fields and known runtime secret values."""
        if isinstance(value, Mapping):
            for key, child in value.items():
                if SENSITIVE_KEY.search(str(key)):
                    raise ValueError("Credential-bearing content is forbidden")
                self.ensure_safe(str(key))
                self.ensure_safe(child)
        elif isinstance(value, list):
            for child in value:
                self.ensure_safe(child)
        elif isinstance(value, str) and self.redact(value) != value:
            raise ValueError("Credential-bearing content is forbidden")


class StructuredFormatter(logging.Formatter):
    def __init__(self, policy: SecretPolicy) -> None:
        super().__init__()
        self._policy = policy

    def format(self, record: logging.LogRecord) -> str:
        fields = {"level": record.levelname, "message": record.getMessage()}
        for key in ("event_id", "request_id", "response_id", "correlation_id"):
            if hasattr(record, key):
                fields[key] = str(getattr(record, key))
        # Redact before encoding so quotes/control characters cannot hide secrets.
        return json.dumps(
            {key: self._policy.redact(value) for key, value in fields.items()}
        )
