import os
import sys

sys.path.insert(0, os.path.abspath(".."))

project = "SlateDB Python API"
extensions = [
    "autoapi.extension",
    "sphinx.ext.napoleon",
]
autoapi_type = "python"
autoapi_dirs = ["../slatedb"]
autoapi_root = "api"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]
html_theme = "sphinx_rtd_theme"
