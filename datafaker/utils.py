"""Utility functions."""
import ast
import importlib.util
import io
import json
import logging
import math
import random
import re
import string
import sys
import typing
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence, Sized
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Final, Generator, Generic, Iterable, TypeVar

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy.engine import make_url

from datafaker.settings import SettingsError

CONFIG_SCHEMA_PATH: Final[Path] = (
    Path(__file__).parent / "json_schemas/config_schema.json"
)

# This is the main logger that the other modules of datafaker should use for output.
# conf_logger() should be called once, as early as possible, to configure this logger.
logger = logging.getLogger("datafaker")

T = TypeVar("T")


class Empty(Generic[T]):
    """Generic empty sequences for default arguments."""

    @classmethod
    def iterable(cls) -> Iterable[T]:
        """Get an empty iterable."""
        e: list[T] = []
        return (x for x in e)


def read_config_file(path: Path) -> dict:
    """Read a config file, warning if it is invalid.

    Args:
        path: The path to a YAML-format config file.

    Returns:
        The config file as a dictionary.
    """
    with path.open("r", encoding="utf8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        logger.error(
            "The config file is invalid, its top level should be an associative array."
        )
        return {}

    schema_config = json.loads(CONFIG_SCHEMA_PATH.read_text(encoding="UTF-8"))
    try:
        validate(config, schema_config)
    except ValidationError as e:
        logger.error("The config file is invalid: %s", e.message)

    return config


def import_file(file_path: str) -> ModuleType:
    """Import a file.

    This utility function returns file_path imported as a module.

    Args:
        file_path (str): The path of a file to import.

    Returns:
        ModuleType
    """
    spec = importlib.util.spec_from_file_location("df", file_path)
    if spec is None or spec.loader is None:
        raise SettingsError(f"No loadable module '{file_path}'")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as e:
        logger.error("Failed to load module at %s with error:", file_path)
        logger.error(e)
    except FileNotFoundError as e:
        raise SettingsError(f"No module found '{file_path}'") from e
    return module


_ASYNC_DRIVER_MAP: dict[str, str] = {
    "postgresql": "postgresql+asyncpg",
    "mssql": "mssql+aioodbc",
}


def make_async_dsn(db_dsn: str) -> str:
    """Return an async-driver DSN for the given sync DSN.

    Replaces the driver component based on the dialect so that both PostgreSQL
    and MS-SQL connections can be made async without hardcoding dialect names at
    each call site.  Raises ``ValueError`` for dialects with no known async driver.
    """
    url = make_url(db_dsn)
    dialect = url.drivername.split("+")[0]
    async_driver = _ASYNC_DRIVER_MAP.get(dialect)
    if async_driver is None:
        raise ValueError(
            f"No async driver is registered for dialect '{dialect}'. "
            f"Add an entry to _ASYNC_DRIVER_MAP in datafaker/utils.py."
        )
    return str(url.set(drivername=async_driver))


def schema_qualified_name(table_name: str, engine: Any) -> str:
    """Return schema-qualified table name using the engine's schema_translate_map.

    When create_db_engine sets schema_translate_map={None: schema_name}, this
    reads it back so raw SQL strings (which schema_translate_map doesn't rewrite)
    can include the correct qualifier.
    """
    schema_map = engine.get_execution_options().get("schema_translate_map", {})
    schema = schema_map.get(None)
    if schema and "." not in table_name:
        return f"{schema}.{table_name}"
    return table_name


def info_or_lower(record: logging.LogRecord) -> bool:
    """Allow records with level of INFO or lower."""
    return record.levelno in (logging.DEBUG, logging.INFO)


def warning_or_higher(record: logging.LogRecord) -> bool:
    """Allow records with level of WARNING or higher."""
    return record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)


class StdoutHandler(logging.Handler):
    """
    A handler that writes to stdout.

    We aren't using StreamHandler because that confuses typer.testing.CliRunner
    """

    def flush(self) -> None:
        """Flush the buffer."""
        self.acquire()
        try:
            sys.stdout.flush()
        finally:
            self.release()

    def emit(self, record: Any) -> None:
        """Write the record."""
        try:
            msg = self.format(record)
            sys.stdout.write(msg + "\n")
            sys.stdout.flush()
        except RecursionError:
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)


class StderrHandler(logging.Handler):
    """
    A handler that writes to stderr.

    We aren't using StreamHandler because that confuses typer.testing.CliRunner
    """

    def flush(self) -> None:
        """Flush the buffer."""
        self.acquire()
        try:
            sys.stderr.flush()
        finally:
            self.release()

    def emit(self, record: Any) -> None:
        """Write the record."""
        try:
            msg = self.format(record)
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except RecursionError:
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)


def conf_logger(verbose: bool) -> None:
    """Configure the logger."""
    # Note that this function modifies the global `logger`.
    log_format = "%(message)s"

    # info will always be printed to stdout
    # debug will be printed to stdout only if verbose=True
    stdout_handler = StdoutHandler()
    stdout_handler.setFormatter(logging.Formatter(log_format))
    stdout_handler.addFilter(info_or_lower)

    # warning and error will always be printed to stderr
    stderr_handler = StderrHandler()
    stderr_handler.setFormatter(logging.Formatter(log_format))
    stderr_handler.addFilter(warning_or_higher)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        handlers=[stdout_handler, stderr_handler],
        force=True,
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("blib2to3.pgen2.driver").setLevel(logging.WARNING)


def get_flag(maybe_dict: Any, key: Any) -> bool:
    """
    Get a boolean from a mapping, or False if that does not make sense.

    :param maybe_dict: A mapping, or possibly not.
    :param key: A key in ``maybe_dict``, or possibly not.
    :return: True only if ``maybe_dict`` is a mapping, ``maybe_dict[key]``
    exists and ``maybe_dict[key]`` is truthy.
    """
    return isinstance(maybe_dict, Mapping) and maybe_dict.get(key, False)


def _element_or_iterable_to_iterable(maybe_it: Any) -> Sequence:
    """
    Get a list of elements from maybe an element.

    :param maybe_it: Either an iterable of elements or just one.
    :return: An iterable of elements, either the argument or a singleton
     of the argument.
    """
    if isinstance(maybe_it, str):
        return [maybe_it]
    if isinstance(maybe_it, Sequence):
        return maybe_it
    return [maybe_it]


def get_property(maybe_dict: Any, key: Any, default: T) -> T:
    """
    Get a specific property from a dict or a default if that does not exist.

    :param maybe_dict: A mapping, or possibly not.
    :param key: A key in ``maybe_dict``, or possibly not. An iterable can
      be passed to chain property fetches through multiple mappings.
    :param default: The return value if ``maybe_dict`` is not a mapping,
      or if ``key`` is not a key of ``maybe_dict``. Do not pass ``None``!
      if you want None as the default, please use get_property_or_none
    :return: ``maybe_dict[key]`` if this makes sense, or ``default`` if not.
    """
    keys = _element_or_iterable_to_iterable(key)
    v = maybe_dict
    for k in keys:
        if isinstance(v, Sequence) and isinstance(v, Sized) and isinstance(k, int):
            if len(v) <= k:
                return default
        elif isinstance(v, Mapping):
            if k not in v:
                return default
        else:
            return default
        v = v[k]
    return v if isinstance(v, type(default)) else default


def _set_property(obj: Any, keys: Sequence[Any], index: int, value: Any) -> bool:
    if len(keys) <= index or not isinstance(obj, MutableMapping):
        return False
    k = keys[index]
    if len(keys) == index + 1:
        obj[k] = value
        return True
    if k not in obj:
        obj[k] = {}
    return _set_property(
        obj[k],
        keys,
        index + 1,
        value,
    )


def set_property(maybe_dict: Any, key: Any, value: Any) -> bool:
    """
    Set a property in a dictionary, or dictionary of dictionaries.

    So, ``set_property(d, "one", 3)`` sets ``d["one"] == 3`` and
    returns True if ``d`` is a mutable mapping, and False (without
    making any changes) otherwise.

    ``set_property(d, ["one", "two"], 3)`` sets ``d["one"]["two"] == 3``
    and returns True if ``d`` is a mutable mapping, ``d["one"]`` exists
    and is a mutable mapping. If ``d["one"]`` does not exist then
    ``d["one"] = {"two": 3}`` is set and True is returned.

    Similarly ``set_property(d, ["one", "two", "three"], 3) sets either
    ``d["one"]["two"]["three"] = 3``, ``d["one"]["two"] = {"three", 3}``
    or ``d["one"] = {"two": {"three": 3}}`` depending on the state
    of ``d``.

    :param maybe_dict: The dictionary to set a value in,
    :param key: The key, or list of keys to descend into the dictionaries.
    :param value: The value to set.
    :return: True if successful, False if any object along the chain
     is not a mutable mapping.
    """
    keys = _element_or_iterable_to_iterable(key)
    return _set_property(maybe_dict, keys, 0, value)


def get_property_or_none(maybe_dict: Any, key: Any, type_: type[T]) -> T | None:
    """
    Get a specific property from a dict or None if that does not exist.

    :param maybe_dict: A mapping, or possibly not.
    :param key: A key in ``maybe_dict``, or possibly not.
    :param type_: The type that the value retrieved should have.
    :return: ``maybe_dict[key]`` if this makes sense, or ``default`` if not.
    """
    if not isinstance(maybe_dict, Mapping) or key not in maybe_dict:
        return None
    v = maybe_dict[key]
    return v if isinstance(v, type_) else None


def get_vocabulary_table_names(config: Mapping) -> set[str]:
    """Extract the table names with a vocabulary_table: true property."""
    return {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "vocabulary_table")
        and not get_flag(table_config, "ignore")
    }


def get_ignored_table_names(config: Mapping) -> set[str]:
    """Extract the table names with a ignore: true property."""
    return {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "ignore")
    }


def get_columns_assigned(
    row_generator_config: Mapping[str, Any]
) -> Generator[str, None, None]:
    """
    Get the columns assigned in a ``row_generators[n]`` stanza.

    :param generator_config: The ``row_generators[n]`` stanza itself.
    """
    ca = row_generator_config.get("columns_assigned", None)
    if ca is None:
        return
    if isinstance(ca, str):
        yield ca
        return
    if not hasattr(ca, "__iter__"):
        return
    for c in ca:
        yield str(c)


def get_row_generators(
    table_config: Mapping[str, Any],
) -> Generator[tuple[str, Mapping[str, Any]], None, None]:
    """
    Get the row generators from a table configuration.

    :param table_config: The element from the ``tables:`` stanza of ``config.xml``.
    :return: Pair of (name, row generator config).
    """
    rgs: list[Any] = get_property(table_config, "row_generators", [])
    for rg in rgs:
        name = get_property_or_none(rg, "name", str)
        if name:
            yield (name, rg)


_alphanumeric_re = re.compile(r"[^a-zA-Z0-9]")


def normalize_table_name(table_name: str) -> str:
    """Remove non alphanumeric characters from table name."""
    name = _alphanumeric_re.sub("_", table_name)
    if not name or not name[0].isalpha():
        return "_" + name
    return name


def make_foreign_key_name(table_name: str, col_name: str) -> str:
    """Make a suitable foreign key name."""
    ntn = normalize_table_name(table_name)
    name = f"{ntn}_{col_name}_fkey"
    # really this should be max_identifier_length in the sqlalchemy dialect
    if len(name) < 64:
        return name
    rand = "".join(random.choice(string.ascii_letters) for _ in range(6))
    return f"{ntn[:24]}_{col_name[:24]}_{rand}_fkey"


def make_primary_key_name(table_name: str) -> str:
    """Make a suitable primary key name."""
    return f"{normalize_table_name(table_name)}_primary_key"


def stream_yaml(yaml_file_handle: io.TextIOBase) -> Generator[Any, None, None]:
    """
    Stream a yaml list into an iterator.

    Used instead of yaml.load(yaml_path) when the file is
    known to be a list and the file might be too long to
    be decoded in memory.
    """
    buf = ""
    while True:
        line = yaml_file_handle.readline()
        if not line or line.startswith("-"):
            if buf:
                yl = yaml.load(buf, yaml.Loader)
                assert isinstance(yl, Sequence) and len(yl) == 1
                yield yl[0]
            if not line:
                return
            buf = ""
        buf += line


def underline_error(e: SyntaxError) -> str:
    r"""
    Make an underline for this error.

    :return: string beginning ``\n`` then spaces then ``^^^^``
    underlining the error, or a null string if this was not possible.
    """
    start = e.offset
    if start is None:
        return ""
    end = e.end_offset
    if end is None or end <= start:
        end = start + 1
    return "\n" + " " * start + "^" * (end - start)


def text_list(items: Iterable[str], empty="") -> str:
    """Concatenate the items with commas and one "and"."""
    item_i = iter(items)
    try:
        last_item = next(item_i)
    except StopIteration:
        return empty
    try:
        so_far = next(item_i)
    except StopIteration:
        return last_item
    for item in item_i:
        so_far += ", " + last_item
        last_item = item
    return so_far + " and " + last_item


def gather_from_ast(
    errors: MutableSequence[tuple],
    name: str,
    value: Any,
    is_for_capture: Callable[[ast.AST], bool],
    node_to_str: Callable[[ast.AST], str],
) -> set[str]:
    """
    Get strings from some part of a Python string.

    :param errors: Output syntax errors.
    :param name: The name of the symbol supplied as ``value``, to be used in
      error messages.
    :param value: The value to be searched; either Python text (as a string),
      a sequence of Python texts, a mapping with Python texts in the values,
      or similar things nested to any depth.
    :param is_for_capture: If this returns True for a particular AST node, it
      will be passed to ``node_to_str`` to provide one string of the output.
    :param node_to_str: Turns each node that passes ``is_for_capture``
      into a string for output.
    :return: The set of symbols found in the Python text(s).
    """
    if isinstance(value, Mapping):
        return set().union(
            *(
                gather_from_ast(
                    errors, f"{name}[{repr(k)}]", v, is_for_capture, node_to_str
                )
                for k, v in value.items()
            )
        )
    if isinstance(value, str):
        try:
            return {
                node_to_str(node)
                for node in ast.walk(ast.parse(value))
                if is_for_capture(node)
            }
        except SyntaxError as e:
            errors.append(
                (
                    "Syntax error in %s: %s\n%s%s",
                    name,
                    e.msg,
                    value,
                    underline_error(e),
                )
            )
        return set()
    if not isinstance(value, Sequence):
        return set()
    return set().union(
        *(
            gather_from_ast(errors, f"{name}[{i}]", v, is_for_capture, node_to_str)
            for i, v in enumerate(value)
        )
    )


def gather_keys_from_mapping(
    errors: MutableSequence[tuple],
    name: str,
    value: Any,
    name_of_mapping: str,
) -> set[str]:
    """
    Get all the literal keys from a mapping in Python texts.

    :param errors: Output syntax errors.
    :param name: The name of the symbol supplied as ``value``, to be used in
      error messages.
    :param value: The value to be searched; either Python text (as a string),
      a sequence of Python texts, a mapping with Python texts in the values,
      or similar things nested to any depth.
    :param name_of_mapping: The mapping to search for.
    :return: The set of symbols found in the Python text(s).
    """

    def is_wanted(node: ast.AST) -> bool:
        """Return True if this node is name_of_mapping["string"]."""
        return (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == name_of_mapping
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        )

    return gather_from_ast(
        errors,
        name,
        value,
        is_wanted,
        lambda node: node.slice.value,  # type: ignore
    )


def gather_symbols(
    errors: MutableSequence[tuple],
    name: str,
    value: Any,
) -> set[str]:
    """
    Get all the symbols from Python texts.

    :param errors: Output syntax errors.
    :param name: The name of the symbol supplied as ``value``, to be used in
      error messages.
    :param value: The value to be searched; either Python text (as a string),
      a sequence of Python texts, a mapping with Python texts in the values,
      or similar things nested to any depth.
    :return: The set of symbols found in the Python text(s).
    """
    return gather_from_ast(
        errors,
        name,
        value,
        lambda node: isinstance(node, ast.Name),
        lambda node: node.id,  # type: ignore
    )


def generators_require_stats(config: Mapping) -> bool:
    """
    Test if the generator references ``SRC_STATS``.

    :param config: ``config.yaml`` object.
    :return: True if any of the arguments for any of the generators
      reference ``SRC_STATS``.
    """
    errors: list[tuple] = []
    symbols = gather_symbols(
        errors,
        "object_instantiation",
        config.get("object_instantiation", {}),
    ).union(
        gather_symbols(
            errors,
            "story_generators",
            config.get("story_generators", []),
        ),
        *(
            gather_symbols(
                errors,
                f"tables[{repr(table_name)}]['row_generators']",
                table.get("row_generators", []),
            )
            for table_name, table in config.get("tables", {}).items()
        ),
        *(
            gather_symbols(
                errors,
                f"tables[{repr(table_name)}]['missingness_generators']",
                table.get("missingness_generators", []),
            )
            for table_name, table in config.get("tables", {}).items()
        ),
    )
    for error in errors:
        logger.error(*error)
    return "SRC_STATS" in symbols


def unqualify_fk_target(fk: str, table_names: typing.Optional[frozenset] = None) -> str:
    """
    Drop the schema qualifier from a 3-part FK target.

    Converts ``schema.table.column`` → ``table.column`` so that SQLAlchemy
    can resolve the reference against a MetaData whose tables were registered
    without a schema prefix. 2-part ``table.column`` targets are returned
    unchanged.

    When ``table_names`` is supplied, a 3-part target whose first two parts
    form a known table name (e.g. ``manufacturer.parquet``) is left unchanged
    because the dot is part of the table name, not a schema prefix.
    """
    parts = fk.split(".")
    if len(parts) == 3:
        if table_names is not None and f"{parts[0]}.{parts[1]}" in table_names:
            return fk
        return f"{parts[1]}.{parts[2]}"
    return fk


def split_column_full_name(col_fullname: str) -> tuple[str, str]:
    """
    Split a column fullname into table and column.

    :param col_fn: The string, such as ``artist.artist_id`` or ``artist.parquet.artist_id``.
    :return: A pair of strings; the table name and the column name. For example
    ``("artist.parquet", "artist_id")``. If there is no ``.`` in ``col_fullname``
    ``(None, col_fullname)`` will be returned.
    """
    name_parts = col_fullname.split(".")
    return (
        ".".join(name_parts[:-1]),
        name_parts[-1],
    )


def procrustes(input_seq: Sequence[T], to_length: int, default: T) -> list[T]:
    """
    Truncate or stretch ``input_seq`` to length ``to_length``.

    :param input_seq: The sequence to be stretched or truncated.
    :param to_length: The length of the output sequence returned.
    :param default: The element to use if the input sequence is empty.
    :return: ``input_seq[:to_length]`` if ``input_seq`` is longer than
     ``to_length``, ``input_seq`` with elements recycled if ``input_seq``
      is too short but not empty, or ``to_length`` repeats of ``default`` if
      ``input_seq`` is empty.
    """
    s = list(input_seq)
    if len(s) == 0:
        return [default] * to_length
    if len(s) < to_length:
        s = s * math.ceil(to_length / len(s))
    return s[:to_length]
