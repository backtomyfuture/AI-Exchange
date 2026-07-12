"""Credentials used only by the explicit database bootstrap process."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr


MAX_MIGRATION_DSN_BYTES = 8192
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")


class MigrationSettingsError(RuntimeError):
    """Safe, non-secret error raised for an invalid bootstrap boundary."""


@dataclass(frozen=True)
class MigrationSettings:
    database_url: SecretStr
    expected_migration_role: str
    expected_runtime_role: str
    target_schema: str


def _reject() -> MigrationSettingsError:
    return MigrationSettingsError("migration_settings_invalid")


def _metadata_snapshot(metadata: object) -> tuple[int, ...]:
    return (
        int(getattr(metadata, "st_dev")),
        int(getattr(metadata, "st_ino")),
        int(getattr(metadata, "st_size")),
        int(getattr(metadata, "st_mtime_ns")),
        int(getattr(metadata, "st_ctime_ns")),
        int(getattr(metadata, "st_uid")),
        stat.S_IMODE(int(getattr(metadata, "st_mode"))),
    )


def _read_bounded(fd: int) -> bytes:
    chunks: list[bytes] = []
    payload_size = 0
    while True:
        chunk = os.read(
            fd,
            min(4096, MAX_MIGRATION_DSN_BYTES + 2 - payload_size),
        )
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        payload_size += len(chunk)
        if payload_size > MAX_MIGRATION_DSN_BYTES + 1:
            raise _reject()


def _read_secret_file(raw_path: str) -> str:
    if not raw_path or raw_path != raw_path.strip() or "\x00" in raw_path:
        raise _reject()

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow == 0:
        raise _reject()
    flags |= nofollow

    fd: int | None = None
    try:
        fd = os.open(raw_path, flags)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise _reject()
        if stat.S_IMODE(before.st_mode) not in {0o400, 0o600}:
            raise _reject()
        if before.st_size <= 0 or before.st_size > MAX_MIGRATION_DSN_BYTES + 1:
            raise _reject()

        payload = _read_bounded(fd)
        middle = os.fstat(fd)
        if _metadata_snapshot(before) != _metadata_snapshot(middle):
            raise _reject()
        os.lseek(fd, 0, os.SEEK_SET)
        repeated_payload = _read_bounded(fd)
        after = os.fstat(fd)
        if payload != repeated_payload or _metadata_snapshot(
            before
        ) != _metadata_snapshot(after):
            raise _reject()
        if payload.endswith(b"\n"):
            payload = payload[:-1]
        if not payload or len(payload) > MAX_MIGRATION_DSN_BYTES:
            raise _reject()
        if b"\x00" in payload or b"\n" in payload or b"\r" in payload:
            raise _reject()
        value = payload.decode("utf-8")
        if value != value.strip():
            raise _reject()
        return value
    except MigrationSettingsError:
        raise
    except (OSError, UnicodeError, ValueError):
        raise _reject() from None
    finally:
        if fd is not None:
            os.close(fd)


def _read_identifier(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise _reject()
    return value


def load_migration_settings(
    environment: Mapping[str, str] | None = None,
) -> MigrationSettings:
    """Load a migration DSN from one private file without runtime fallback."""

    values = os.environ if environment is None else environment
    try:
        database_url = _read_secret_file(values.get("MIGRATION_DATABASE_URL_FILE", ""))
        migration_role = _read_identifier(values, "POSTGRES_MIGRATION_OWNER_ROLE")
        runtime_role = _read_identifier(values, "POSTGRES_RUNTIME_ROLE")
        target_schema = _read_identifier(values, "POSTGRES_SCHEMA")
        if migration_role == runtime_role:
            raise _reject()

        parsed = conninfo_to_dict(database_url)
        if (
            parsed.get("user") != migration_role
            or not parsed.get("password")
            or not parsed.get("host")
            or not parsed.get("dbname")
            or parsed.get("options") != f"-csearch_path={target_schema}"
        ):
            raise _reject()
    except MigrationSettingsError:
        raise
    except Exception:
        raise _reject() from None

    return MigrationSettings(
        database_url=SecretStr(database_url),
        expected_migration_role=migration_role,
        expected_runtime_role=runtime_role,
        target_schema=target_schema,
    )
