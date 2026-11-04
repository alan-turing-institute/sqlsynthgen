"""Sphinx configuration"""
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import pathlib
import sys

from sphinx.application import Sphinx

sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix())
sys.path.insert(0, os.path.abspath("../.."))

project = "datafaker"  # pylint: disable=C0103
copyright = "2023, anon"  # pylint: disable=C0103,W0622
author = "anon"  # pylint: disable=C0103
release = "0.0"  # pylint: disable=C0103

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions: list[str] = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_rtd_theme",
    "sphinx.ext.napoleon",
    "sphinxcontrib.mermaid",
]

autodoc_mock_imports: list[str] = ["typer", "pydantic", "sqlalchemy"]

templates_path: list[str] = ["_templates"]
exclude_patterns: list[str] = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"  # pylint: disable=C0103
html_static_path = ["_static"]


def setup(app: Sphinx) -> None:
    """Include our own CSS in rendered pages."""
    app.add_css_file("custom.css")
