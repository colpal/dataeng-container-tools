#!/usr/bin/env python3
"""Sphinx configuration for DE Container Tools documentation."""

import datetime

from dataeng_container_tools import __version__

# Project information
project = "DE Container Tools"
copyright = f"{datetime.datetime.now(tz=datetime.timezone.utc).year}, Colgate-Palmolive"
author = "CP DE Team"
version = __version__
release = version

# General configuration
extensions = [
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "autoapi.extension",
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "sphinx_tabs.tabs",
]

templates_path = ["_templates"]
exclude_patterns = []
source_suffix = [".rst", ".md"]
master_doc = "index"

# HTML output configuration
html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_title = f"DE Container Tools {version}"
html_logo = "_static/logo-light.svg"
html_favicon = "_static/favicon.ico"
html_theme_options = {
    "navigation_depth": 4,
    "logo": {
        "image_dark": "_static/logo-dark.svg",
    },
}

# Extension configuration
suppress_warnings = ["autoapi.python_import_resolution"]
tls_verify = False

autoapi_dirs = ["../dataeng_container_tools"]
autoapi_options = [
    "members",
    "inherited-members",
    # "undoc-members",  # Causes issues with imports
    "private-members",
    "special-members",
    "show-inheritance",
    # "show-inheritance-diagram",
    "show-module-summary",
    "imported-members",
]
autoapi_member_order = "groupwise"
autoapi_python_class_content = "class"
autoapi_keep_files = False
autoapi_add_toctree_entry = True

# Napoleon settings for Google-style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
}

# Copybutton configuration
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

# MyST configuration for markdown support
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_heading_anchors = 3
