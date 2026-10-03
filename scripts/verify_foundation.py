"""Retry-safe local verification against a disposable database."""

from isolated_postgres import isolated_postgres

from events_service.runtime_check import verify

if __name__ == "__main__":
    with isolated_postgres() as database:
        verify(database)
    print("PostgreSQL foundation verification passed")
