"""PostgreSQL connection helper shared by the load, validate, and benchmark modules."""
import psycopg

from src.config import db_params


def connect() -> psycopg.Connection:
    """Open a connection using the configured (environment-provided) credentials.

    Use as a context manager: the transaction commits on success and rolls back
    on any exception.
    """
    return psycopg.connect(**db_params())
