import os
import sys
sys.path.insert(0, os.path.abspath('../python'))

project = 'SlateDB Python API'
extensions = [
    'autoapi.extension',
    'sphinx.ext.napoleon',
]
autoapi_type = 'python'
autoapi_dirs = ['../python']
autoapi_root = 'api'
html_theme = 'sphinx_rtd_theme'
