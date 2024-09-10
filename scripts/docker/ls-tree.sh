#!/bin/bash

# list contents of current directory as a tree
find . | sed -e "s/[^-][^\/]*\// |/g" -e "s/|\([^ ]\)/|-\1/"