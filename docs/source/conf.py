# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../../python'))


# -- Project information -----------------------------------------------------

project = 'Mosaic'
copyright = '2024, Databricks Inc'
author = 'Milos Colic, Stuart Lynn, Michael Johns, Robert Whiffin'

# The full version, including alpha/beta/rc tags
release = "v0.4.2"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "nbsphinx",
    "sphinx_tabs.tabs",
    "sphinx.ext.githubpages",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.todo"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', ".env"]
source_suffix = [".rst", ".md"]

pygments_style = 'sphinx'
nbsphinx_execute = 'never'
napoleon_use_admonition_for_notes = True
sphinx_tabs_disable_tab_closing = True
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'renku'

# Material theme options (see theme.conf for more information)
html_theme_options = {

    # Set the name of the project to appear in the navigation.
    'nav_title': f'Mosaic {release}',

    # Specify a base_url used to generate sitemap.xml. If not
    # specified, then no sitemap will be built.
    # 'base_url': 'https://project.github.io/project',

    # Set the color and the accent color
    'color_primary': 'green',
    'color_accent': 'green',

    # Set the repo location to get a badge with stats
    'repo_url': 'https://github.com/databrickslabs/mosaic/',
    'repo_name': 'Mosaic',

    'globaltoc_depth': 3,
    'globaltoc_collapse': False,
    'globaltoc_includehidden': True,    
    'heroes': {'index': 'Simple, scalable geospatial analytics on Databricks',
               'examples/index': 'examples and tutorials to get started with '
                                 'Mosaic'},
    "version_dropdown": True,
    # "version_json": "../versions-v2.json",

}
html_title = project
html_short_title = project
html_logo = 'images/mosaic_logo.svg'
html_sidebars = {
    "**": ["logo-text.html", "globaltoc.html", "localtoc.html", "searchbox.html"]
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ["css/custom.css"]
