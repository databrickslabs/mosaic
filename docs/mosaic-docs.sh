#!/bin/bash

# ::: Refer to 'CONTRIBUTING.md' for more tips :::
# - make sure pandoc is installed, see https://pandoc.org/installing.html

# [1] upgrade pip
pip install --upgrade pip

# [2] build docs requirements
pip install -r docs-requirements.txt

# [3] build docs
make html

# [4] run `reload.py`
# - uncomment, this is best run through IntelliJ
#python3 "$(pwd)/source/reload.py"