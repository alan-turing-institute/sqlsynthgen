"""Tests for the remove module."""
from unittest.mock import MagicMock, call, patch

from sqlsynthgen.remove import remove_db_data
from tests.examples import example_orm, remove_ssg
from tests.utils import SSGTestCase


class RemoveTestCase(SSGTestCase):
    """Tests for removing data, vocabs and tables."""

    @patch("sqlsynthgen.remove.create_db_engine")
    @patch("sqlsynthgen.remove.delete", side_effect=(9, 8, 7, 6))
    def test_remove_db_data(
        self, mock_delete: MagicMock, mock_engine: MagicMock
    ) -> None:
        """Test the remove_db_data function."""
        remove_db_data(example_orm, remove_ssg)
        mock_delete.assert_has_calls(
            [
                call(example_orm.Base.metadata.tables["myschema." + t])
                for t in ("hospital_visit", "test_entity", "person", "entity")
            ]
        )
        dst_engine = mock_engine.return_value
        dst_conn = dst_engine.connect.return_value.__enter__.return_value
        dst_conn.execute.assert_has_calls([call(x) for x in (9, 8, 7, 6)])

    def test_remove_db_vocab(self) -> None:
        """Test the remove_db_vocab function."""
        # ToDo

    def test_remove_tables(self) -> None:
        """Test the remove_db_tables function."""
        # ToDo
