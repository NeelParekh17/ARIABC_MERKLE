"""Pytest configuration for the Merkle recovery test suite.

pytest_addoption must live in conftest.py (not in the test module) so that
pytest registers the option before it parses CLI arguments.
"""

from __future__ import annotations

import argparse

import pytest


def pytest_addoption(parser) -> None:
    group = parser.getgroup("merkle recovery")
    group.addoption(
        "--dsn",
        action="store",
        default="host=127.0.0.1 port=5432 dbname=postgres user=neel",
        help="PostgreSQL DSN for Merkle recovery tests",
    )
