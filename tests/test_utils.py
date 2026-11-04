"""Tests for the utils module."""
import os
import sys
import tempfile
from importlib import resources
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from sqlalchemy import Column, Integer, insert
from sqlalchemy.orm import declarative_base

from datafaker.db_utils import download_table
from datafaker.utils import (
    generators_require_stats,
    import_file,
    procrustes,
    read_config_file,
)
from tests.utils import DatafakerTestCase, RequiresDBTestCase

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class MyTable(Base):
    """A SQLAlchemy model."""

    __tablename__ = "mytable"
    id = Column(
        Integer,
        primary_key=True,
    )


class TestImport(DatafakerTestCase):
    """Tests for the import_file function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def test_import_file(self) -> None:
        """Test that we can import an example module."""
        old_path = sys.path.copy()
        module = import_file("import_test.py")
        self.assertEqual(10, module.x)

        self.assertEqual(old_path, sys.path)


class TestDownload(RequiresDBTestCase):
    """Tests for the download_table function."""

    dump_file_path = "providers.dump"
    mytable_file_path = Path("mytable.yaml")

    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()

        metadata.create_all(self.engine)

        self.test_dir = Path(tempfile.mkdtemp(prefix="df-"))
        os.chdir(self.test_dir)
        self.mytable_file_path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)
        super().tearDown()

    def test_download_table(self) -> None:
        """Test the download_table function."""
        # pylint: disable=protected-access

        with self.sync_engine.connect() as conn:
            conn.execute(insert(MyTable).values({"id": 1}))
            conn.commit()

        download_table(
            MyTable.__table__, self.sync_engine, self.mytable_file_path, compress=False
        )

        # The .strip() gets rid of any possible empty lines at the end of the file.
        tests_module = sys.modules["tests"]
        with resources.as_file(
            resources.files(tests_module) / "examples" / "expected.yaml"
        ) as yamlpath:
            with yamlpath.open(encoding="utf-8") as yamlfile:
                expected = yamlfile.read().strip()

        with self.mytable_file_path.open(encoding="utf-8") as yamlfile:
            actual = yamlfile.read().strip()

        self.assertEqual(expected, actual)


class TestReadConfig(DatafakerTestCase):
    """Tests for the read_config_file function."""

    def test_warns_of_invalid_config(self) -> None:
        """Test that we get a warning if the config is invalid."""
        with patch("datafaker.utils.logger") as mock_logger:
            read_config_file(Path("tests/examples/invalid_config.yaml"))
            mock_logger.error.assert_called_with(
                "The config file is invalid: %s", "'a' is not of type 'integer'"
            )


class TestUtils(DatafakerTestCase):
    """Miscellaneous tests."""

    def test_generators_require_stats(self) -> None:
        """Test that we can tell if a configuration requires SRC_STATS or not."""
        self.assertTrue(
            generators_require_stats(
                {
                    "object_instantiation": {
                        "mygen": {
                            "name": "MyGen",
                            "kwargs": {"a": '1 + SRC_STATS["my"]["results"][0]'},
                        }
                    }
                }
            )
        )
        self.assertTrue(
            generators_require_stats(
                {
                    "story_generators": [
                        {
                            "name": "msg",
                            "kwargs": {"a": '[None] + SRC_STATS["my"]["results"]'},
                        }
                    ]
                }
            )
        )
        self.assertTrue(
            generators_require_stats(
                {
                    "story_generators": [
                        {
                            "name": "msg",
                            "args": ['(SRC_STATS["my"]["results"])'],
                        }
                    ]
                }
            )
        )
        self.assertTrue(
            generators_require_stats(
                {
                    "tables": {
                        "things": {
                            "missingness_generators": [
                                {
                                    "name": "msg",
                                    "kwargs": {
                                        "a": '[SRC_STATS["my"], SRC_STATS["theirs"]]'
                                    },
                                    "columns_assigned": ["a"],
                                }
                            ]
                        }
                    }
                }
            )
        )
        self.assertTrue(
            generators_require_stats(
                {
                    "tables": {
                        "things": {
                            "row_generators": [
                                {
                                    "name": "MyGen",
                                    "kwargs": {"a": 'SRC_STATS["ifu"]["results"]'},
                                    "columns_assigned": ["a"],
                                }
                            ]
                        }
                    }
                }
            )
        )
        self.assertTrue(
            generators_require_stats(
                {
                    "tables": {
                        "things": {
                            "row_generators": [
                                {
                                    "name": "MyGen",
                                    "args": ["SRC_STATS"],
                                    "columns_assigned": ["a"],
                                }
                            ]
                        }
                    }
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "object_instantiation": {
                        "mygen": {"name": "MyGen", "kwargs": {"a": 1}}
                    }
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "story_generators": [
                        {
                            "name": "msg",
                            "kwargs": {"a": "[None]"},
                        }
                    ]
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "story_generators": [
                        {
                            "name": "msg",
                            "args": ['(SRC_STATS_["my"]["results"])'],
                        }
                    ]
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "missingness_generators": [
                        {
                            "name": "msg",
                            "kwargs": {"a": '"SRC_STATS"'},
                            "columns_assigned": ["a"],
                        }
                    ]
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "tables": {
                        "things": {
                            "row_generators": [
                                {
                                    "name": "MyGen",
                                    "kwargs": {"a": 'SRC_STAT["ifu"]["results"]'},
                                    "columns_assigned": ["a"],
                                }
                            ]
                        }
                    }
                }
            )
        )
        self.assertFalse(
            generators_require_stats(
                {
                    "tables": {
                        "things": {
                            "row_generators": [
                                {
                                    "name": "MyGen",
                                    "args": ["SRC_STATSS"],
                                    "columns_assigned": ["a"],
                                }
                            ]
                        }
                    }
                }
            )
        )

    @patch("datafaker.utils.logger")
    def test_testing_generators_finds_syntax_errors(self, logger: MagicMock) -> None:
        """Test that looking for ``SRC_STATS`` references finds Python syntax errors."""
        generators_require_stats(
            {
                "story_generators": [
                    {"name": "my_story_gen", "kwargs": {"b": "'unclosed"}}
                ],
                "tables": {
                    "things": {
                        "row_generators": [
                            {
                                "name": "MyGen",
                                "args": ["1 2"],
                                "columns_assigned": ["a"],
                            }
                        ]
                    }
                },
            }
        )
        logger.error.assert_has_calls(
            [
                call(
                    "Syntax error in %s: %s\n%s%s",
                    "story_generators[0]['kwargs']['b']",
                    "unterminated string literal (detected at line 1)",
                    "'unclosed",
                    "\n ^",
                ),
                call(
                    "Syntax error in %s: %s\n%s%s",
                    "tables['things']['row_generators'][0]['args'][0]",
                    "invalid syntax",
                    "1 2",
                    "\n   ^",
                ),
            ]
        )

    def test_procrustes(self) -> None:
        """Test the ``procrustes`` function."""
        self.assertListEqual(procrustes([], 4, 5), [5, 5, 5, 5])
        self.assertListEqual(procrustes([1], 4, 5), [1, 1, 1, 1])
        self.assertListEqual(procrustes([1, 2, 3], 8, 5), [1, 2, 3, 1, 2, 3, 1, 2])
        self.assertListEqual(
            procrustes([1, 100, -2, -3, 4, -10, -1, 1], 4, 5), [1, 100, -2, -3]
        )
