"""Put the generated values into the database, obeying other restrictions."""
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlalchemy
from mimesis import Generic
from mimesis.locales import Locale

from datafaker.base import ColumnPresence
from datafaker.db_utils import constraint_name
from datafaker.make import FunctionCall, RowGeneratorInfo, TableGeneratorInfo
from datafaker.providers import (
    AnchoredProvider,
    BytesProvider,
    ColumnValueProvider,
    DistributionProvider,
    ExtractProvider,
    NullProvider,
    SQLGroupByProvider,
    TimedeltaProvider,
    TimespanProvider,
    WeightedBooleanProvider,
)
from datafaker.utils import gather_keys_from_mapping, import_file, logger


def make_generic(metadata: sqlalchemy.MetaData) -> Generic:
    """Make the generic provider instance."""
    g = Generic(locale=Locale.EN_GB)
    g.add_providers(
        BytesProvider,
        ColumnValueProvider,
        DistributionProvider,
        ExtractProvider,
        NullProvider,
        SQLGroupByProvider,
        TimedeltaProvider,
        TimespanProvider,
        WeightedBooleanProvider,
    )
    g.add_provider(AnchoredProvider, metadata=metadata)
    return g


def _eval_structure(config: Any, context: Mapping) -> Any:
    """
    Turn a structure from ``config.yaml`` into a Python object.

    :param config: a structure (list, dict, number or expression in a string).
    :return: Object matching the structure of ``config`` with strings eval'ed.
    """
    if isinstance(config, str):
        try:
            # pylint: disable=eval-used
            return eval(config, None, context)
        except KeyError as exc:
            logger.error("Python Error in %s: %s", config, exc)
            raise exc
        except Exception as exc:
            logger.error("Failed to parse Python: %s", config)
            raise exc
    if isinstance(config, Mapping):
        return {k: _eval_structure(v, context) for k, v in config.items()}
    if isinstance(config, Sequence):
        return [_eval_structure(v, context) for v in config]
    return config


def _get_object(class_name: str, context: Mapping) -> Any:
    """
    Fetch an object from the context.

    :param class_name: The name of the class, qualified if necessary.
      Like "module.MyClass.Nested"
    :param context: Mapping of strings to objects with those names.
    :return: A value from ``context`` if there are no qualifying names,
    otherwise the attribute of the base object.
    """
    parts = class_name.split(".")
    if parts[0] not in context:
        raise ValueError(
            f'No such object "{parts[0]}" (symbols available are {", ".join(context.keys())})'
        )
    value = context[parts[0]]
    so_far = parts[0]
    for part in parts[1:]:
        so_far += "." + part
        if not hasattr(value, part):
            raise ValueError(f'No such attribute "{so_far}"')
        value = getattr(value, part)
    return value


def _call_from_context(
    callable_name: str, args: Sequence[Any], kwargs: Mapping[str, Any], context: Mapping
) -> Any:
    """
    Call a callable from the classes (or functions) in the context.

    :param class_name: Possibly qualified name of class to construct.
    :param context: Mapping of base classes and modules
    :return: Constructed object, or None if this did not work.
    """
    cls = _get_object(callable_name, context)
    if not callable(cls):
        return None
    arg_objs = [_eval_structure(arg, context) for arg in args]
    kwarg_objs = {k: _eval_structure(v, context) for k, v in kwargs.items()}
    return cls(*arg_objs, **kwarg_objs)


def call_function(fn: FunctionCall, context: Mapping) -> Any:
    """Call ``fn`` within the provided context."""
    return _call_from_context(
        fn.function_name,
        fn.args,
        fn.kwargs,
        context,
    )


def get_symbols(
    row_generator_module_name: str | None,
    story_generator_module_name: str | None,
    object_instantiation: dict[str, dict[str, Any]] | None,
    src_stats: dict[str, dict[str, Any]] | None,
    metadata: sqlalchemy.MetaData,
) -> dict[str, Any]:
    """Get the symbols that may be referred to by various configuration settings."""
    generic = make_generic(metadata)
    symbols: dict[str, Any] = {
        "metadata": metadata,
        "generic": generic,
        "numeric": generic.numeric,
        "person": generic.person,
        "dist_gen": DistributionProvider(),
        "column_presence": ColumnPresence(),
    }
    _get_symbol_import(symbols, row_generator_module_name)
    _get_symbol_import(symbols, story_generator_module_name)
    if object_instantiation:
        _get_symbols_instantiation(symbols, object_instantiation)
    if src_stats is not None:
        symbols["SRC_STATS"] = src_stats
    return symbols


def _get_symbol_import(symbols: dict[str, Any], module_name: str | None) -> None:
    """
    Load a module and add it as a symbol.

    :param symbols: Dict to add the module to.
    :param module_name: if None, nothing will be added to ``symbols``.
      Otherwise the ``module_name`` module will be loaded and added as
      ``symbols[module_name]``.
    """
    if module_name is None:
        return
    symbols[module_name] = import_file(module_name + ".py")


def _get_symbols_instantiation(symbols: dict[str, Any], objs: dict[str, Any]) -> None:
    """
    Instantiate objects and add them to the ``symbols`` dictionary.

    :param symbols: Dict to add the new objects to; also the context for the
      instantiations.
    :param objs: Dict of names to instantiation configurations. The names are
      the keys that will be added to ``symbols``, the values are each callable
      named by ``objs[name]["class"]`` with the arguments provided by
      ``objs[name]["kwargs"]`` (which is a dict of argument names to a
      Python string of the value to pass to that argument, such as ``'0'`` for
      the number zero or ``"hello"`` for the string "hello").
    """
    for name, inst in objs.items():
        clbl = inst.get("class", None)
        args = inst.get("args", [])
        kwargs = inst.get("kwargs", {})
        if (
            isinstance(clbl, str)
            and isinstance(kwargs, dict)
            and isinstance(args, list)
        ):
            symbols[name] = _call_from_context(clbl, args, kwargs, symbols)


@dataclass
class RowGenAndRelated:
    """
    A ``RowGeneratorInfo`` together with all the columns related to it.

    The columns related are indices to ``ROW_GENERATORS`` found in the
    ``args`` and ``kwargs`` attributes for the row generator.
    """

    row_gen: RowGeneratorInfo
    related_columns: set[str]


class TableGenerator:
    """Puts generated values into a destination table."""

    def __init__(
        self,
        dst_db_conn: sqlalchemy.Connection,
        table_data: TableGeneratorInfo,
        max_unique_constraint_tries: int | None,
    ) -> None:
        """
        Initialize a table generator.

        :param rows_per_pass: How many rows to add for each call to ``__call__``.
        :param dst_db_conn: Connection to the destination database.
        :param table_data: Configuration for this generator.
        :param max_unique_constraint_tries: How many times to redo generation in
          an attempt to satisfy uniqueness constraints. None means never stop, but
          this could cause an infinite loop if there are no solutions, or very long
          execution if there are few solutions with many constraints.
        """
        self.table_data = table_data
        self.max_unique_constraint_tries = max_unique_constraint_tries
        self.existing_constraint_hashes: MutableMapping[str, set[int]] = {}
        self.context: MutableMapping = {}
        with dst_db_conn.begin():
            for constraint in table_data.unique_constraints:
                expr = sqlalchemy.select(*constraint.columns)
                query_result = dst_db_conn.execute(expr).fetchall()
                self.existing_constraint_hashes[constraint_name(constraint)] = {
                    hash(tuple(result)) for result in query_result
                }
        errors: list[tuple] = []
        self.row_gens = [
            RowGenAndRelated(
                rg,
                gather_keys_from_mapping(
                    errors,
                    f"table['{table_data.table_name}']['row_generators']",
                    {"args": rg.function_call.args, "kwargs": rg.function_call.kwargs},
                    "GENERATED_ROW",
                ),
            )
            for rg in table_data.row_gens
        ]
        for error in errors:
            logger.warning(error)

    @property
    def num_rows_per_pass(self) -> int:
        """Get the number of rows this generator should produce relative to all the rest."""
        return self.table_data.rows_per_pass

    @property
    def name(self) -> str:
        """Get the name of the table whose rows we are generating."""
        return self.table_data.table_name

    def set_context(self, context: Mapping) -> None:
        """Set all the Python symbols that must be known to the configuration."""
        self.context = {**context}

    def generate_row(self, _db_conn: sqlalchemy.Connection) -> dict[str, Any]:
        """Generate some rows of the relevant table in the database."""
        result: dict[str, Any] = {}
        self.context["GENERATED_ROW"] = result
        columns_to_generate = set(self.table_data.nonnull_columns)
        # Which missingness patterns do we want?
        for choice in self.table_data.column_choices:
            cols = _call_from_context(
                choice.function_name, choice.args, choice.kwargs, self.context
            )
            columns_to_generate.update(cols)

        max_tries = self.max_unique_constraint_tries
        while columns_to_generate:
            if max_tries == 0:
                raise RuntimeError(
                    "Failed to satisfy unique constraints for table"
                    f" {self.table_data.table_name} after"
                    f" {self.max_unique_constraint_tries} attempts."
                )
            if max_tries is not None:
                max_tries -= 1
            for rg in self.row_gens:
                row_gen = rg.row_gen
                if (
                    # if we need to generate at least one column this generator generates
                    set(row_gen.variable_names) & columns_to_generate
                    # and if we no longer need to generate any related columns
                    and not (rg.related_columns & columns_to_generate)
                ):
                    values = call_function(
                        row_gen.function_call,
                        self.context,
                    )
                    if len(row_gen.variable_names) == 1:
                        result[row_gen.variable_names[0]] = values
                    else:
                        for index, variable_name in enumerate(row_gen.variable_names):
                            result[variable_name] = values[index]
                    columns_to_generate -= set(row_gen.variable_names)
            for constraint in self.table_data.unique_constraints:
                cf_hash = hash(tuple(result[col.name] for col in constraint.columns))
                if (
                    cf_hash
                    in self.existing_constraint_hashes[constraint_name(constraint)]
                ):
                    columns_to_generate.update(c.name for c in constraint.columns)
        for constraint in self.table_data.unique_constraints:
            cf_hash = hash(tuple(result[col.name] for col in constraint.columns))
            self.existing_constraint_hashes[constraint_name(constraint)].add(cf_hash)
        return result


def get_table_generator_dict(
    dst_db_conn: sqlalchemy.Connection,
    tables_data: Iterable[TableGeneratorInfo],
    max_unique_constraint_tries: int | None,
) -> dict[str, TableGenerator]:
    """Get a dict of table names to row generators that generate rows for that table."""
    return {
        table_data.table_name: TableGenerator(
            dst_db_conn,
            table_data,
            max_unique_constraint_tries,
        )
        for table_data in tables_data
    }
