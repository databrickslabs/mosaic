#!/bin/bash

# run from project root
# - this will also install dependencies
pip install --upgrade pip
pip install --upgrade build setuptools wheel
pip install -v .
python3 -m build . --wheel