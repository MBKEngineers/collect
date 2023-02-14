# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import datetime as dt
import os
import sys
sys.path.insert(0, os.path.abspath('.')) # or '..' for rtd theme


# -- Project information -----------------------------------------------------

project = 'collect'
copyright = f'{dt.datetime.now():%Y}, MBK Engineers'
author = 'MBK Engineers'

# The full version, including alpha/beta/rc tags
release = '0.1'


# -- General configuration ---------------------------------------------------

# include both class docstring and __init__
autoclass_content = 'both'

# Make sure that any autodoc declarations show the right members
autodoc_default_flags = ['members',
                         #'inherited-members',
                         'private-members',
                         'show-inheritance']
autosummary_generate = True  # Make _autosummary files and include them

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc', 
              'sphinx.ext.coverage', 
              'sphinx.ext.napoleon', 
              'sphinx.ext.autosummary',
              'sphinx.ext.intersphinx']#,
              # 'sphinx.ext.linkcode']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Napoleon settings
# napoleon_numpy_docstring = False  # Force consistency, leave only Google
# napoleon_use_rtype = False  # More legible

# html_sidebars = {
#    '**': ['globaltoc.html', 'sourcelink.html', 'searchbox.html'],
#    'using/windows': ['windowssidebar.html', 'searchbox.html'],
# }

html_use_index = True 


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
# html_theme = 'alabaster'
html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'collapse_navigation': False,
    'sticky_navigation': False,
    'navigation_depth': 6,
    'includehidden': True,
    'titles_only': False
}

# import sphinx_readable_theme
# html_theme_path = [sphinx_readable_theme.get_html_theme_path()]
# html_theme = 'readable'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

def setup(app):
    app.add_stylesheet('css/custom.css')