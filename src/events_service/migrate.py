"""Apply packaged Alembic migrations without logging a credential-bearing DSN."""

import argparse
import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text


def migrate(url: str, revision: str = "head", *, downgrade: bool = False) -> None:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    engine = create_engine(url, hide_parameters=True)
    try:
        with engine.begin() as connection:
            # Serialize every migration invocation, including concurrent deployments.
            # The transaction releases the lock on success or failure; lock waits
            # are bounded so a stuck deploy does not hold the release indefinitely.
            connection.execute(text("SET LOCAL lock_timeout = '60s'"))
            connection.execute(text("SELECT pg_advisory_xact_lock(461, 1)"))
            config.attributes["connection"] = connection
            operation = command.downgrade if downgrade else command.upgrade
            operation(config, revision)
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("revision", nargs="?", default="head")
    parser.add_argument("--downgrade", action="store_true")
    args = parser.parse_args()
    try:
        migrate(os.environ["WES_DATABASE_URL"], args.revision, downgrade=args.downgrade)
    except Exception:
        raise SystemExit(
            "Migration failed; check database configuration and revision"
        ) from None


if __name__ == "__main__":
    main()
