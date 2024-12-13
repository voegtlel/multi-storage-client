#
# Sphinx configuration.
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#

from sphinx_pyproject import SphinxConfig

# Disabled due to https://github.com/sphinx-toolbox/sphinx-pyproject/issues/59.
#
# Load `name`, `version`, `description`, and `authors` from the project's `pyproject.toml`.
#
# Prefer defining other configurations in this file instead of using
# `[tool.sphinx-pyproject]` in the project's `pyproject.toml`.
# config = SphinxConfig(
#     "../../pyproject.toml",
#     globalns=globals(),
#     style="poetry"
# )
#
# project = config.name
# release = config.version
project = "multi-storage-client"
author = "NVIDIA Multi-Storage Client Team"
copyright = "NVIDIA Corporation"

# Extensions.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

# Themes.
html_theme = "furo"

# Syntax highlighting. `pygments_dark_style` is specific to the Furo theme.
pygments_style = "solarized-light"
pygments_dark_style = "solarized-dark"

# Line numbers.
viewcode_line_numbers = True

# Docstrings.
autoclass_content = "both"
autodoc_typehints = "both"
