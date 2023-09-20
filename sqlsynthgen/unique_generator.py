"""Module for the UniqueGenerator class."""
from typing import Any, Callable, List, Optional, Set

import sqlalchemy as sqla

from sqlsynthgen.utils import logger


class UniqueGenerator:
    """Class to ensure values generated for given columns are unique.

    There should be one instance of UniqueGenerator for each unique constraint.

    The generation works by repeatedly generating new values until a unique one is
    found. Old values already in the database are loaded to memory when the instance is
    first called to generate values.

    Attributes:
        column_names (List[str]): Columns to which the unique constraint applies.
        table_name (str): The name of the table.
        max_tries (int): The maximum number of attempts to generate a unique key.
    """

    def __init__(self, columns: List[str], table_name: str, max_tries: int = 100):
        """
        Initialise a UniqueGenerator.

        Args:
            column_names (List[str]): Columns to which the unique constraint applies.
            table_name (str): The name of the table.
            max_tries (int): The maximum number of attempts to generate a unique key.
        """
        self.existing_keys: Optional[Set[Any]] = None
        self.column_names = columns
        self.table_name = table_name
        self.max_tries = max_tries

    def get_existing_keys(self, dst_db_conn: sqla.Connection) -> set:
        """
        Retrieve existing keys from the database.

        Args:
            dst_db_conn: The connection to the destination database.

        Returns:
            keys (set): A set of existing keys retrieved from the database.
        """
        query_text = f"SELECT {','.join(self.column_names)} FROM {self.table_name}"
        query_result = dst_db_conn.execute(sqla.text(query_text)).fetchall()
        keys = set(query_result)
        return keys

    def __call__(
        self,
        dst_db_conn: sqla.Connection,
        columns_assigned: List[str],
        inner_generator: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Generate a unique value by calling an inner generator function.

        Repeatedly calls `inner_generator` with the provided `args` and `kwargs` until a
        value that fulfills the given unique constraint is found. Raises a RuntimeError
        if no such value is found after `self.max_tries` attempts.

        Args:
            dst_db_conn: Connection to the destination database.
            columns_assigned (List[str]): Names of columns to which inner_generator's
                output will be assigned. Used to deterimine which outputs of
                inner_generator are relevant for the constraint.
            inner_generator: The generator function that we wrap.
            *args: Variable length argument list passed to `inner_generator`.
            **kwargs: Arbitrary keyword arguments passed to `inner_generator`.

        Returns:
            `inner_generator(*args, **kwargs)`, but with the unique constraint
            guaranteed.

        Raises:
            RuntimeError: If a unique value cannot be generated after
            `self.max_tries` attempts.
        """
        if self.existing_keys is None:
            self.existing_keys = self.get_existing_keys(dst_db_conn)

        # Check if inner_generator returns multiple values.
        single_output = len(columns_assigned) == 1
        enforceable = True
        output_indices: List[int] = []
        if not single_output:
            # Find the indices of the outputs of inner_generator that this constraint
            # concerns. E.g. if the output of inner_generator is to be assigned to
            # columns a, b, and c, and there's a uniqueness constraint on a and c, then
            # output_indices should be (0, 2).
            for constraint_column in self.column_names:
                try:
                    output_indices.append(columns_assigned.index(constraint_column))
                except ValueError:
                    # This means that one of the constraint columns isn't part of the
                    # output of inner_generator. This means we can't guarantee enforcing
                    # the constraint.
                    enforceable = False
                    logger.warning("Unenforceable unique constraint")
                    break

        for _ in range(self.max_tries):
            candidate_value = inner_generator(*args, **kwargs)
            if not enforceable:
                # We bypass the logic of trying to enforce the constraint at all.
                return candidate_value
            if single_output:
                candidate_key: Any = (candidate_value,)
            else:
                # Take the part of the return value of inner_generator that concerns
                # this unique constraint.
                candidate_key = tuple(candidate_value[i] for i in output_indices)
            if candidate_key not in self.existing_keys:
                self.existing_keys.add(candidate_key)
                return candidate_value
        msg = (
            "Failed to generate a value that satisfies unique constraint for "
            f"{self.column_names} in {self.table_name} after {self.max_tries} attempts."
        )
        raise RuntimeError(msg)
