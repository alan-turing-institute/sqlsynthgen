"""Tests for the remove module."""
from unittest.mock import MagicMock, call, patch

from datafaker.remove import remove_db_data, remove_db_tables, remove_db_vocab
from datafaker.settings import Settings
from tests.examples import example_orm, remove_datafaker
from tests.utils import DatafakerTestCase, get_test_settings


class RemoveTestCase(DatafakerTestCase):
    """Tests for removing data, vocabs and tables."""

    @patch("datafaker.remove.get_settings", side_effect=get_test_settings)
    @patch("datafaker.remove.create_db_engine")
    @patch("datafaker.remove.delete", side_effect=range(1, 9))
    def test_remove_db_data(
        self, mock_delete: MagicMock, mock_engine: MagicMock, _: MagicMock
    ) -> None:
        """Test the remove_db_data function."""
        config = {"tables": {"unignorable_table": {"ignore": True}}}
        remove_db_data(example_orm, remove_datafaker, config)
        self.assertEqual(mock_delete.call_count, 8)
        mock_delete.assert_has_calls(
            [
                call(example_orm.Base.metadata.tables[t])
                for t in (
                    "hospital_visit",
                    "test_entity",
                    "unique_constraint_test2",
                    "unique_constraint_test",
                    "person",
                    "strange_type_table",
                    "no_pk_test",
                    "data_type_test",
                )
            ],
            any_order=True,
        )
        dst_engine = mock_engine.return_value
        dst_conn = dst_engine.connect.return_value.__enter__.return_value
        dst_conn.execute.assert_has_calls([call(x) for x in range(1, 8)])

    @patch("datafaker.remove.get_settings")
    def test_remove_db_data_raises(self, mock_get: MagicMock) -> None:
        """Check that remove_db_data raises if dst DSN is missing."""
        mock_get.return_value = Settings(
            dst_dsn=None,
            # The mypy ignore can be removed once we upgrade to pydantic 2.
            _env_file=None,  # type: ignore[call-arg]
        )
        with self.assertRaises(AssertionError) as context_manager:
            remove_db_data(example_orm, remove_datafaker, {})
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )

    @patch("datafaker.remove.get_settings", side_effect=get_test_settings)
    @patch("datafaker.remove.create_db_engine")
    @patch("datafaker.remove.delete", side_effect=range(1, 6))
    def test_remove_db_vocab(
        self, mock_delete: MagicMock, mock_engine: MagicMock, _: MagicMock
    ) -> None:
        """Test the remove_db_vocab function."""
        config = {"tables": {"unignorable_table": {"ignore": True}}}
        remove_db_vocab(example_orm, remove_datafaker, config)
        self.assertEqual(mock_delete.call_count, 5)
        mock_delete.assert_has_calls(
            [
                call(example_orm.Base.metadata.tables[t])
                for t in (
                    "concept",
                    "ref_to_unignorable_table",
                    "concept_type",
                    "mitigation_type",
                    "empty_vocabulary",
                )
            ],
            any_order=True,
        )
        dst_engine = mock_engine.return_value
        dst_conn = dst_engine.connect.return_value.__enter__.return_value
        dst_conn.execute.assert_has_calls([call(x) for x in range(1, 6)])

    @patch("datafaker.remove.get_settings")
    def test_remove_db_vocab_raises(self, mock_get: MagicMock) -> None:
        """Check that remove_db_vocab raises if dst DSN is missing."""
        mock_get.return_value = Settings(
            dst_dsn=None,
            # The mypy ignore can be removed once we upgrade to pydantic 2.
            _env_file=None,  # type: ignore[call-arg]
        )
        with self.assertRaises(AssertionError) as context_manager:
            remove_db_vocab(example_orm, remove_datafaker, {})
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )

    @patch("datafaker.remove.get_settings", side_effect=get_test_settings)
    @patch("datafaker.remove.create_db_engine")
    def test_remove_tables(self, mock_engine: MagicMock, _: MagicMock) -> None:
        """Test the remove_db_tables function."""
        mock_orm = MagicMock()
        remove_db_tables(mock_orm, {})
        dst_engine = mock_engine.return_value
        mock_orm.Base.metadata.drop_all.assert_called_once_with(dst_engine)

    @patch("datafaker.remove.get_settings")
    def test_remove_db_tables_raises(self, mock_get: MagicMock) -> None:
        """Check that remove_db_tables raises if dst DSN is missing."""
        mock_get.return_value = Settings(
            dst_dsn=None,
            # The mypy ignore can be removed once we upgrade to pydantic 2.
            _env_file=None,  # type: ignore[call-arg]
        )
        with self.assertRaises(AssertionError) as context_manager:
            remove_db_tables(example_orm, {})
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )
