# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


import os
import sys
from importlib.metadata import version

# Define path to the code to be documented **relative to where conf.py (this file) is kept**
sys.path.insert(0, os.path.abspath("../src/"))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "hats"
copyright = "2025, LINCC Frameworks"
author = "LINCC Frameworks"
release = version("hats")
# for example take major/minor
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinx_design",
    "numpydoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
]

extensions.append("autoapi.extension")
extensions.append("nbsphinx")
extensions.append("myst_parser")

# -- sphinx-copybutton configuration ----------------------------------------
extensions.append("sphinx_copybutton")
## sets up the expected prompt text from console blocks, and excludes it from
## the text that goes into the clipboard.
copybutton_exclude = ".linenos, .gp"
copybutton_prompt_text = ">> "

## lets us suppress the copy button on select code blocks.
copybutton_selector = "div:not(.no-copybutton) > div.highlight > pre"

templates_path = []
exclude_patterns = ["_build", "**.ipynb_checkpoints"]

# This assumes that sphinx-build is called from the root directory
master_doc = "index"
# Remove 'view source code' from top of page (for html, not python)
html_show_sourcelink = False
# Remove namespaces from class/method signatures
add_module_names = False

autoapi_type = "python"
autoapi_dirs = ["../src"]
autoapi_ignore = ["*/__main__.py", "*/_version.py"]
autoapi_add_toc_tree_entry = False
autoapi_member_order = "bysource"
# Additional configuration to skip private members
autoapi_python_class_content = "class"
autoapi_generate_api_docs = True
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]

html_theme = "sphinx_book_theme"
html_theme_options = {
    "repository_url": "https://github.com/astronomy-commons/hats",
    "use_repository_button": True,
}

html_static_path = ["_static"]
html_css_files = ["custom.css"]


def skip_private_members(app, what, name, obj, skip, options):
    """Skip private members during autoapi generation."""
    # Get just the member name (including the module path if present)
    # and skip if any component is private (starts with a single underscore).
    member_name = name.split(".")[-1] if "." in name else name
    if member_name.startswith("_") and not member_name.startswith("__"):
        return True  # Force skip private members

    # For non-private members, use the default behavior
    return skip


def setup(app):
    """Set up the Sphinx app with custom configurations."""
    app.connect("autoapi-skip-member", skip_private_members)
