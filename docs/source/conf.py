# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import pathlib
import sys
import sphinx_rtd_theme
sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix())
print("Path 0: {}".format(pathlib.Path(__file__).parents[0]))
print("Path 1: {}".format(pathlib.Path(__file__).parents[1]))
print("Path 2: {}".format(pathlib.Path(__file__).parents[2]))

import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

project = 'sqlsynthgen'
copyright = '2023, anon'
author = 'anon'
release = '0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx_rtd_theme',
    ]

autodoc_mock_imports = ["typer", "pydantic", "mimesis"]

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
# html_sidebars = {
#     '**': [
#         'about.html',
#         'navigation.html',
#         'relations.html',
#         'searchbox.html',
#         'donate.html',
#     ]
# }