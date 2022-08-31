import os
import sys

# Put the project package on the path
sys.path.insert(0, os.path.abspath('..'))

project = 'boto3-helpers'
copyright = '2022, Bo Bayles'
author = 'Bo Bayles'

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.viewcode']
autodoc_member_order = 'bysource'
templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
