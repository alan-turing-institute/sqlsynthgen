"""Run the .rst linter via a unit test.

The CLI does not allow errors to be disabled, but we can ignore them here."""
from pathlib import Path
from unittest import TestCase

from restructuredtext_lint import lint_file


class RstTests(TestCase):
    """Linting for the doc .rst files."""

    def test_dir(self) -> None:
        """Run the linter on the docs/ directory."""
        docs_path = Path("docs/")
        rst_files = docs_path.glob("**/*.rst")

        all_errors = []
        for rst_file in rst_files:
            all_errors.append(lint_file(str(rst_file)))

        # Ignore errors if they contain any of these strings
        allowed_errors = [
            'No role entry for "ref" in module',
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
        ]
        filtered_errors = [
            file_error
            for file_errors in all_errors
            for file_error in file_errors
            # Only worry about ERRORs and WARNINGs
            if file_error.level <= 2
            if not any(filter(lambda m: m in file_error.full_message, allowed_errors))
        ]

        if filtered_errors:
            self.fail(msg="\n".join([
                f"{err.source}({err.line}): {["Severe", "Error", "Warning"][err.level]}: {err.full_message}"
                for err in filtered_errors
            ]))
