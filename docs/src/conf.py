#
# Sphinx configuration.
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#

from sphinx_pyproject import SphinxConfig

# Load `name`, `version`, `description`, and `authors` from the project's `pyproject.toml`.
#
# Prefer defining other configurations in this file instead of using
# `[tool.sphinx-pyproject]` in the project's `pyproject.toml`.
config = SphinxConfig(
    "../../pyproject.toml",
    globalns=globals(),
)

project = config.name
release = config.version
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
