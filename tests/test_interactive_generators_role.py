""" Tests for the configure-generators role command. """
from collections.abc import MutableMapping
from typing import Any

from datafaker.interactive.base import DbCmd
from datafaker.utils import get_property
from tests.test_interactive_generators import MockGeneratorCmd
from tests.utils import RequiresDBTestCase


class ConfigureRolesTests(RequiresDBTestCase):
    """Testing configure-generators' role command."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the command we are using for this test case."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def test_set_roles(self) -> None:
        """Test that we can set roles for a particular column."""
        with self._get_cmd({}) as gc:
            table = "model"
            column = "name"
            roles = {"start", "source"}
            gc.do_next(f"{table}.{column}")
            for role in roles:
                gc.do_role(f"set {role}")
            gc.do_quit("")
            actual_roles = get_property(
                gc.config,
                ["tables", table, "columns", column, "roles"],
                [],
            )
            self.assertSetEqual(set(actual_roles), roles)

    def test_delete_roles(self) -> None:
        """Test that we can set roles for a particular column."""
        table = "model"
        column = "name"
        with self._get_cmd(
            {"tables": {table: {"columns": {column: {"roles": ["start", "source"]}}}}}
        ) as gc:
            gc.do_next(f"{table}.{column}")
            gc.do_role("delete source")
            gc.do_quit("")
            actual_roles = get_property(
                gc.config,
                ["tables", table, "columns", column, "roles"],
                [],
            )
            self.assertListEqual(actual_roles, ["start"])

    def _get_roles_from_role_list(self, gc: MockGeneratorCmd) -> set[str]:
        """Get the roles in the named column from the ``role`` command."""
        gc.reset()
        gc.do_role("list")
        roles = gc.messages[0][0].split(", ")
        if roles == [""]:
            return set()
        return set(roles)

    def test_list_roles(self) -> None:
        """
        Test that we can see roles for a particular column.

        Tests both the ``columns`` and ``role list``.
        """
        with self._get_cmd({}) as gc:
            table = "model"
            column = "name"
            gc.do_next(f"{table}.{column}")
            gc.reset()
            gc.do_role("list")
            self.assertEqual(gc.messages[0][0], gc.NO_ROLES_TEXT)
            gc.do_role("set start")
            self.assertSetEqual(self._get_roles_from_role_list(gc), {"start"})
            self.assertSetEqual(gc.get_roles_from_columns(column), {"start"})
            gc.do_role("set source")
            self.assertSetEqual(self._get_roles_from_role_list(gc), {"start", "source"})
            self.assertSetEqual(gc.get_roles_from_columns(column), {"start", "source"})
