"""Tests for the main module."""
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import yaml
from sqlalchemy import Column, Integer, create_engine, insert
from sqlalchemy.orm import declarative_base

from sqlsynthgen import make
from tests.examples import example_orm
from tests.utils import run_psql

# pylint: disable=protected-access
# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class MakeTable(Base):  # type: ignore
    """A SQLAlchemy table."""

    __tablename__ = "maketable"
    id = Column(
        Integer,
        primary_key=True,
    )


class MyTestCase(TestCase):
    """Module test case."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers",
        )
        metadata.create_all(self.engine)
        os.chdir("tests/examples")

    def tearDown(self) -> None:
        os.chdir("../..")

    def test_make_generators_from_tables(self) -> None:
        """Check that we can make a generators file from a tables module."""
        self.maxDiff = None  # pylint: disable=invalid-name
        with open("expected_ssg.py", encoding="utf-8") as expected_output:
            expected = expected_output.read()
        conf_path = "generator_conf.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)

        with patch("sqlsynthgen.make._download_table",) as mock_download, patch(
            "sqlsynthgen.make.create_engine"
        ) as mock_create_engine, patch("sqlsynthgen.make.get_settings"):
            actual = make.make_generators_from_tables(example_orm, config)
            mock_download.assert_called_once()
            # self.assertEqual(, mock_download.call_args_list[0])
            mock_create_engine.assert_called_once()

        self.assertEqual(expected, actual)

    def test__download_table(self) -> None:
        """Test the _download_table function."""
        with self.engine.connect() as conn:
            conn.execute(insert(MakeTable).values({"id": 1}))

        make._download_table(MakeTable.__table__, self.engine)

        with Path("expected.csv").open(encoding="utf-8") as csvfile:
            expected = csvfile.read()

        with Path("maketable.csv").open(encoding="utf-8") as csvfile:
            actual = csvfile.read()

        self.assertEqual(expected, actual)
