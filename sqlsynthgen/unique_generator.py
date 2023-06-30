from typing import Iterable

import sqlalchemy as sqla


class UniqueGenerator:
    def __init__(self, columns, table_name, max_tries=100):
        self.existing_keys = None
        self.columns = columns
        self.table_name = table_name
        self.max_tries = max_tries

    def get_existing_keys(self, dst_db_conn):
        query_text = f"SELECT {','.join(self.columns)} FROM {self.table_name}"
        query_result = dst_db_conn.execute(sqla.text(query_text)).fetchall()
        keys = set(query_result)
        return keys

    def __call__(
        self,
        dst_db_conn,
        output_indices: Iterable[int],
        inner_generator,
        *args,
        **kwargs,
    ):
        if self.existing_keys is None:
            self.existing_keys = self.get_existing_keys(dst_db_conn)

        for _ in range(self.max_tries):
            candidate_value = inner_generator(*args, **kwargs)
            # Take the part of the return value of inner_generator that concerns this
            # unique constraint.
            candidate_key = tuple(candidate_value[i] for i in output_indices)
            if candidate_key not in self.existing_keys:
                self.existing_keys.add(candidate_key)
                return candidate_value
        msg = (
            "Failed to generate a value that satisfies unique constraint for "
            "{} in {} after {} attempts."
        )
        raise RuntimeError(msg, self.columns, self.table_name, self.max_tries)
