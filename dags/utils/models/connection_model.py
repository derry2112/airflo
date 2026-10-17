"""Connection fields used by the local transfer hooks."""

from dataclasses import dataclass


@dataclass
class ConnectionModel:
    username_dap: str = ""
    host: str | None = None
    port: int | None = None
    schema: str | None = None
    password_dap: str | None = None
