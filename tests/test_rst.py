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
            'Hyperlink target "enduser" is not referenced',
        ]
        filtered_errors = []
        for file_errors in all_errors:
            for file_error in file_errors:
                skip = False

                error_message = file_error.full_message

                for allowed_error in allowed_errors:
                    if allowed_error in error_message:
                        skip = True
                        break

                if skip:
                    continue

                filtered_errors.append(file_error)

        # Only worry about ERRORs and WARNINGs
        level_one_errors = [x.full_message for x in filtered_errors if x.level <= 2]
        self.assertListEqual([], level_one_errors)
