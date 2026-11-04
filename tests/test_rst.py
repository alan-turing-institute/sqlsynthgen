"""Run the .rst linter via a unit test.

The CLI does not allow errors to be disabled, but we can ignore them here."""
from pathlib import Path
from typing import Any
from unittest import TestCase

from docutils.parsers.rst import directives
from restructuredtext_lint import lint_file
from sphinxcontrib.mermaid import Mermaid


def _level_to_string(level: int) -> str:
    """Get a string description of an error level."""
    return ["Severe", "Error", "Warning"][level]


def _error_message(lint_error: Any) -> str:
    """Turn a linting error into an error message."""
    source = getattr(lint_error, "source")
    line = getattr(lint_error, "line")
    level = _level_to_string(getattr(lint_error, "level"))
    message = getattr(lint_error, "full_message")
    return f"{source}({line}): {level}: {message}"


class RstTests(TestCase):
    """Linting for the doc .rst files."""

    def test_dir(self) -> None:
        """Run the linter on the docs/ directory."""
        docs_path = Path("docs/")
        rst_files = docs_path.glob("**/*.rst")
        directives.register_directive("mermaid", Mermaid)

        all_errors = []
        for rst_file in rst_files:
            all_errors.append(lint_file(str(rst_file)))

        # Ignore errors if they contain any of these strings
        allowed_errors = [
            'No role entry for "ref" in module',
            'No role entry for "doc" in module',
            'No directive entry for "toctree"',
            'No directive entry for "automodule"',
            'No directive entry for "literalinclude"',
            'Hyperlink target "page-introduction" is not referenced',
            'Hyperlink target "source-statistics" is not referenced',
            'Hyperlink target "page-example-loan-data" is not referenced',
            'Hyperlink target "page-index" is not referenced.',
            'Hyperlink target "page-example-health-data" is not referenced.',
            'Hyperlink target "page-quickstart" is not referenced.',
            'Hyperlink target "page-installation" is not referenced.',
            'Hyperlink target "story-generators" is not referenced.',
            'Hyperlink target "mimesis-string-list" is not referenced',
            'Hyperlink target "increment-proposer" is not referenced',
            'Hyperlink target "evaluating-and-ranking-proposals" is not referenced',
            'Hyperlink target "statistical-fidelity-criteria" is not referenced',
        ]
        filtered_errors = [
            file_error
            for file_errors in all_errors
            for file_error in file_errors
            # Only worry about ERRORs and WARNINGs
            if file_error.level <= 2
            if not any(m in file_error.full_message for m in allowed_errors)
        ]

        if filtered_errors:
            self.fail(msg="\n".join(map(_error_message, filtered_errors)))
